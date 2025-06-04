package service

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/WlayRay/ElectricSearch/util"

	etcdv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
)

type IServiceHub interface {
	Register(group, endpoint string) error     // 注册服务
	UnRegister(group, endpoint string) error   // 注销服务
	GetServiceEndpoints(group string) []string // 服务发现
	GetServiceEndpoint(group string) string    // 根据负载均衡获取一台服务的endpoint
	Close()                                    // 关闭etcd连接
}

// 代理模式，对ServiceHub做一层代理，提供缓存和限流保护
type ServiceHubProxy struct {
	*ServiceHub // ServiceHubProxy拥有ServiceHub的所有方法（匿名成员变量）
	// 服务端地址缓存
	endpointCache sync.Map //维护每一个service下的所有servers
	limiter       *rate.Limiter
}

var (
	proxy     *ServiceHubProxy
	proxyOnce sync.Once
)

func GetServiceHubProxy(etcdEndpoints []string, heartRate int64, qps int) *ServiceHubProxy {
	if proxy == nil {
		proxyOnce.Do(func() {
			serviceHub := GetServiceHub(etcdEndpoints, heartRate)
			proxy = &ServiceHubProxy{
				ServiceHub:    serviceHub,
				endpointCache: sync.Map{},
				limiter:       rate.NewLimiter(rate.Every(time.Duration(1e9/qps)*time.Nanosecond), qps), //每隔1E9/qps纳秒产生一个令牌，即一秒钟之内产生qps个令牌。令牌桶的容量为qps
			}
		})
	}
	return proxy
}

func (proxy *ServiceHubProxy) watchEndpointsOfGroup(group string) {
	if _, exists := proxy.watched.LoadOrStore(group, true); exists {
		return
	}

	prefix := strings.TrimRight(ServiceRootPath, "/") + indexName + "/" + group + "/"
	watchChan := proxy.client.Watch(context.Background(), prefix, etcdv3.WithPrefix())
	util.Info("watch group: %s", group)

	go func() {
		for response := range watchChan {
			for _, event := range response.Events {
				util.Debug("etcd event type: %s", event.Type)

				path := strings.Split(string(event.Kv.Key), "/")
				if len(path) < 3 {
					util.Error("invalid key format: %s", event.Kv.Key)
					continue
				}
				group := path[len(path)-2]
				endpoints := proxy.ServiceHub.GetServiceEndpoints(group)
				if len(endpoints) > 0 {
					proxy.endpointCache.Store(group, endpoints)
				} else {
					proxy.endpointCache.Delete(group)
				}
			}
		}
	}()
}

func (proxy *ServiceHubProxy) GetServiceEndpoints(group string) []string {
	if !proxy.limiter.Allow() {
		util.Error("rate limit exceeded for group: %s", group)
	}

	proxy.watchEndpointsOfGroup(group)
	if endpoints, exists := proxy.endpointCache.Load(group); exists {
		return endpoints.([]string)
	} else {
		endpoints := proxy.ServiceHub.GetServiceEndpoints(group)
		if len(endpoints) > 0 {
			proxy.endpointCache.Store(group, endpoints)
		}
		return endpoints
	}
}
