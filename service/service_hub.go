package service

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/WlayRay/ElectricSearch/etcd"
	"github.com/WlayRay/ElectricSearch/util"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

// 服务注册中心
type ServiceHub struct {
	client       *etcdv3.Client
	heartRate    int64    // server 每间隔heartRate向etcd发送心跳，同时续约
	watched      sync.Map // 存储已经监听过的service
	loadBalancer LoadBalancer
}

// 使用单例模式创建ServiceHub，包外需通过GetServiceHub获取实例
var (
	serviceHub *ServiceHub
)

func GetServiceHub(etcdEndpoints []string, heartRate int64) *ServiceHub {
	if serviceHub == nil {
		if etcdClient, err := etcd.GetEtcdClient(etcdEndpoints); err != nil {
			util.Panic("etcd client init failed: %v", err)
		} else {
			serviceHub = &ServiceHub{
				client:       etcdClient,
				heartRate:    heartRate,
				loadBalancer: &RoundRobin{}, // TODO 将使用的负载均衡算法放到配置文件中
			}
		}
	}
	return serviceHub
}

func (Hub *ServiceHub) Register(group, endpoint string) error {
	// 使用上下文控制整个注册过程
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 创建租约
	lease, err := Hub.client.Grant(timeoutCtx, Hub.heartRate)
	if err != nil {
		util.Error("create lease failed: %v", err)
		return err
	}

	// 注册服务
	keys := ServiceRootPath + indexName + "/" + group + "/" + endpoint
	if _, err := Hub.client.Put(timeoutCtx, keys, "", etcdv3.WithLease(lease.ID)); err != nil {
		util.Error("register service %s endpoint %s failed: %v", group, endpoint, err)
		return err
	}

	// 保持租约
	keepAliveChan, err := Hub.client.KeepAlive(context.TODO(), lease.ID)
	if err != nil {
		util.Error("keep alive failed: %v", err)
		return err
	}

	go func() {
		for kaResp := range keepAliveChan {
			if kaResp == nil {
				util.Error("keep alive channel closed for %s/%s", group, endpoint)
				return
			}
			util.Info("keep alive success for %s/%s, ID: %d", group, endpoint, kaResp.ID)
		}
	}()

	return nil
}

// 注销服务
func (Hub *ServiceHub) UnRegister(group, endpoint string) error {
	timeoutCtx, cancel := util.GetDefaultTimeoutContext()
	defer cancel()

	key := ServiceRootPath + indexName + "/" + group + "/" + endpoint
	if _, err := Hub.client.Delete(timeoutCtx, key); err != nil {
		util.Error("unregister worker %s endpoint %s failed: %v", group, endpoint, err)
		return err
	} else {
		util.Info("unregister worker %s endpoint %s success", group, endpoint)
		return nil
	}
}

// 服务发现，client每次进行RPC调用之前都查询etcd，获取server集合
func (Hub *ServiceHub) GetServiceEndpoints(group string) []string {
	timeoutCtx, cancel := util.GetDefaultTimeoutContext()
	defer cancel()

	prefix := ServiceRootPath + indexName + "/" + group
	if resp, err := Hub.client.Get(timeoutCtx, prefix, etcdv3.WithPrefix()); err != nil {
		util.Error("get group %s endpoints failed: %v", group, err)
		return nil
	} else if resp.Count != 0 {
		endpoints := make([]string, 0, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			path := strings.Split(string(kv.Key), "/") // 只需要key，不需要value
			endpoints = append(endpoints, path[len(path)-1])
		}
		util.Info("now the %s group has endpoints: %v", group, endpoints)
		return endpoints
	} else {
		return nil
	}
}

// 根据负载均衡，从众多endpoint中选择一个
func (Hub *ServiceHub) GetServiceEndpoint(group string) string {
	return Hub.loadBalancer.Take(Hub.GetServiceEndpoints(group))
}

// 关闭etcd客户端连接
func (Hub *ServiceHub) Close() {
	_ = Hub.client.Close()
}

func (Hub *ServiceHub) addIndexGroup() int {
	timeoutCtx, cancel := util.GetDefaultTimeoutContext()
	defer cancel()

	// 获取分布式锁
	lock, err := etcd.AcquireDistributedLock(Hub.client, ServiceRootPath+indexName+"/group-lock", 10, 100*time.Millisecond, 2)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := lock.Release(); err != nil {
			util.Fatal("failed to release lock: %v", err)
		}
	}()

	key := ServiceRootPath + indexName + "/total-shards"
	if res, err := Hub.client.Get(timeoutCtx, key, etcdv3.WithPrefix()); err == nil {
		if res == nil {
			return 0
		}

		var (
			count int
			err   error
		)
		if len(res.Kvs) > 0 {
			value := res.Kvs[0].Value
			count, err = strconv.Atoi(string(value))
		}
		if err != nil {
			util.Error("failed to convert value to int: %v", err)
			return 0
		}
		count++
		_, err = Hub.client.Put(timeoutCtx, key, strconv.Itoa(count))
		if err != nil {
			util.Error("failed to put updated value to etcd: %v", err)
			return 0
		}
		return count
	}
	return 0
}

func (Hub *ServiceHub) subIndexGroup() {
	timeoutCtx, cancel := util.GetDefaultTimeoutContext()
	defer cancel()

	// 获取分布式锁
	lock, err := etcd.AcquireDistributedLock(Hub.client, ServiceRootPath+indexName+"/group-lock", 10, 100*time.Millisecond, 2)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := lock.Release(); err != nil {
			util.Error("failed to release lock: %v", err)
		}
	}()

	key := ServiceRootPath + indexName + "/total-shards"
	if res, err := Hub.client.Get(timeoutCtx, key, etcdv3.WithPrefix()); err == nil {
		if res == nil {
			return
		}
		value := res.Kvs[0].Value
		count, err := strconv.Atoi(string(value))
		if err != nil {
			util.Error("failed to convert value to int: %v", err)
			return
		}
		count--
		if count <= 0 {
			_, _ = Hub.client.Delete(timeoutCtx, key)
			return
		}
		_, err = Hub.client.Put(timeoutCtx, key, strconv.Itoa(count))
		if err != nil {
			util.Error("failed to put updated value to etcd: %v", err)
			return
		}
		return
	}
}

func (Hub *ServiceHub) CountIndexGroup() int {
	timeoutCtx, cancel := util.GetDefaultTimeoutContext()
	defer cancel()

	// 获取分布式锁
	lock, err := etcd.AcquireDistributedLock(Hub.client, ServiceRootPath+indexName+"/group-lock", 10, 100*time.Millisecond, 2)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := lock.Release(); err != nil {
			util.Error("failed to release lock: %v", err)
		}
	}()

	key := ServiceRootPath + indexName + "/total-shards"
	if res, err := Hub.client.Get(timeoutCtx, key, etcdv3.WithPrefix()); err == nil {
		if res == nil {
			return 0
		}
		if res.Count > 0 {
			value := res.Kvs[0].Value
			count, err := strconv.Atoi(string(value))
			if err != nil {
				util.Error("failed to convert value to int: %v", err)
				return 0
			}
			return count
		}
	}
	return 0
}
