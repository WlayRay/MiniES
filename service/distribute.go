package service

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WlayRay/ElectricSearch/etcd"
	"github.com/dgryski/go-farm"
	etcdv3 "go.etcd.io/etcd/client/v3"

	"github.com/WlayRay/ElectricSearch/types"
	"github.com/WlayRay/ElectricSearch/util"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type Sentinel struct {
	Hub      IServiceHub
	seed     int
	connPool sync.Map
}

func NewSentinel(etcdServers []string) *Sentinel {
	return &Sentinel{
		// Hub: GetServiceHub(etcdServers, 10), //直接访问ServiceHub
		Hub:      GetServiceHubProxy(etcdServers, 10, 100), //走代理HubProxy
		connPool: sync.Map{},
		seed:     0,
	}
}

func (sentinel *Sentinel) GetGrpcConn(endpoint string) *grpc.ClientConn {
	if v, exists := sentinel.connPool.Load(endpoint); exists {
		conn := v.(*grpc.ClientConn)
		// 检查连接状态是否为Ready
		if conn.GetState() != connectivity.Ready {
			util.Log.Error("sentinel %s is not ready (state: %v), close it", endpoint, conn.GetState())
			_ = conn.Close()
			sentinel.connPool.Delete(endpoint)
		} else {
			return conn
		}
	}

	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		util.Log.Error("dial %s failed: %s", endpoint, err)
		return nil
	}
	util.Log.Info("successfully connected to grpc server %s", endpoint)
	sentinel.connPool.Store(endpoint, conn)
	return conn
}

func (sentinel *Sentinel) AddDoc(doc types.Document) (int, error) {
	groupIndex := sentinel.getGroupIndex(doc.Id)
	endpoints := sentinel.Hub.GetServiceEndpoints(fmt.Sprintf("group-%d", groupIndex))
	if len(endpoints) == 0 {
		return 0, fmt.Errorf("there is no group can be used")
	}

	var total uint32
	var wg sync.WaitGroup
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func(string) {
			defer wg.Done()
			conn := sentinel.GetGrpcConn(endpoint)
			if conn != nil {
				client := NewIndexServiceClient(conn)
				affected, err := client.AddDoc(context.Background(), &doc)
				if err != nil {
					util.Log.Error("add doc %s to worker %s failed: %s", doc.Id, endpoint, err)
				} else {
					atomic.AddUint32(&total, uint32(affected.Count))
				}
			}

		}(endpoint)
	}

	wg.Wait()
	util.Log.Debug("add doc %s to workers %v, affected %d", doc.Id, endpoints, total)
	return int(total), nil
}

func (sentinel *Sentinel) DeleteDoc(docId string) int {
	groupIndex := sentinel.getGroupIndex(docId)
	endpoints := sentinel.Hub.GetServiceEndpoints(fmt.Sprintf("group-%d", groupIndex))
	if len(endpoints) == 0 {
		return 0
	}

	var total uint32
	wg := sync.WaitGroup{}
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func(string) {
			defer wg.Done()
			conn := sentinel.GetGrpcConn(endpoint)
			if conn != nil {
				client := NewIndexServiceClient(conn)
				affected, err := client.DeleteDoc(context.Background(), &DocId{docId})
				if err != nil {
					util.Log.Error("delete doc %s from worker %s failed: %s", docId, endpoint, err)
				} else if affected.Count > 0 {
					atomic.AddUint32(&total, affected.Count)
				}
			}
		}(endpoint)
	}

	wg.Wait()
	util.Log.Debug("add delete %s to workers %v, affected %d", docId, endpoints, total)
	return int(total)
}

func (sentinel *Sentinel) Search(querys *types.TermQuery, onFlag, offFlag uint64, orFlags []uint64) []*types.Document {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	docs := make([]*types.Document, 0, 1500)
	groupCount := sentinel.getGroupCount()
	if groupCount == 0 {
		return nil
	}

	// 使用多个通道来收集结果
	resultChs := make([]chan *types.Document, groupCount)
	for i := range resultChs {
		resultChs[i] = make(chan *types.Document, 300)
	}

	var producerWg sync.WaitGroup
	producerWg.Add(groupCount)

	// 生产者：向通道发送数据
	for i := range groupCount {
		group := fmt.Sprintf("group-%d", i)
		endpoints := sentinel.Hub.GetServiceEndpoints(group)
		if len(endpoints) == 0 {
			producerWg.Done() // 跳过空组
			continue
		}

		endpoint := sentinel.Hub.GetServiceEndpoint(group)
		go func(endpoint string, resultCh chan<- *types.Document) {
			defer producerWg.Done()
			conn := sentinel.GetGrpcConn(endpoint)
			if conn == nil {
				util.Log.Error("failed to get connection for endpoint %s", endpoint)
				return
			}

			client := NewIndexServiceClient(conn)
			result, err := client.Search(ctx, &SearchRequest{
				Query:   querys,
				OnFlag:  onFlag,
				OffFlag: offFlag,
				OrFlags: orFlags,
			})

			if err != nil {
				util.Log.Error("search from worker %s failed: %s", endpoint, err)
				return
			}

			for _, doc := range result.Documents {
				select {
				case resultCh <- doc:
				case <-ctx.Done():
					return
				}
			}
		}(endpoint, resultChs[i])
	}

	var consumerWg sync.WaitGroup
	consumerWg.Add(groupCount)
	mu := sync.Mutex{}

	for i := range groupCount {
		go func() {
			defer consumerWg.Done()
			for doc := range resultChs[i] {
				mu.Lock()
				docs = append(docs, doc)
				mu.Unlock()
			}
		}()
	}

	// 等待所有生产者完成并关闭通道
	go func() {
		producerWg.Wait()
		for _, ch := range resultChs {
			close(ch)
		}
	}()

	// 等待所有消费者完成
	consumerWg.Wait()

	return docs
}

func (sentinel *Sentinel) Count() int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var n uint32
	groupCount := sentinel.getGroupCount()
	if groupCount == 0 {
		return 0
	}

	// 使用多个通道来收集结果
	resultChs := make([]chan uint32, groupCount)
	for i := range resultChs {
		resultChs[i] = make(chan uint32, 300)
	}

	var producerWg sync.WaitGroup
	producerWg.Add(groupCount)

	// 生产者：向通道发送数据
	for i := range groupCount {
		group := fmt.Sprintf("group-%d", i)
		endpoints := sentinel.Hub.GetServiceEndpoints(group)
		if len(endpoints) == 0 {
			producerWg.Done() // 跳过空组
			continue
		}

		endpoint := sentinel.Hub.GetServiceEndpoint(group)
		go func(endpoint string, resultCh chan<- uint32) {
			defer producerWg.Done()
			conn := sentinel.GetGrpcConn(endpoint)
			if conn == nil {
				util.Log.Error("failed to get connection for endpoint %s", endpoint)
				return
			}

			client := NewIndexServiceClient(conn)
			result, err := client.Count(ctx, &CountRequest{})
			if err != nil {
				util.Log.Error("count from worker %s failed: %s", endpoint, err)
				return
			}

			select {
			case resultCh <- result.Count:
			case <-ctx.Done():
				return
			}
		}(endpoint, resultChs[i])
	}

	var consumerWg sync.WaitGroup
	consumerWg.Add(groupCount)

	for i := range groupCount {
		go func() {
			defer consumerWg.Done()
			for count := range resultChs[i] {
				atomic.AddUint32(&n, count)
			}
		}()
	}

	// 等待所有生产者完成并关闭通道
	go func() {
		producerWg.Wait()
		for _, ch := range resultChs {
			close(ch)
		}
	}()

	// 等待所有消费者完成
	consumerWg.Wait()

	return int(n)
}

func (sentinel *Sentinel) Close() (err error) {
	sentinel.connPool.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*grpc.ClientConn); ok {
			err = conn.Close()
		}
		return true
	})
	sentinel.Hub.Close()
	return
}

// getGroupCount 获取当前索引的分组数量
func (*Sentinel) getGroupCount() int {
	var etcdServers []string
	for _, v := range util.ConfigMap["etcd"].(map[string]any)["servers"].([]any) {
		etcdServers = append(etcdServers, v.(string))
	}

	etcdConn, err := etcd.GetEtcdClient(etcdServers)
	if err != nil {
		util.Log.Fatal("get etcd client failed: %s", err)
	}

	timeoutCtx, cancel := util.GetDefaultTimeoutContext()
	defer cancel()

	resp, getErr := etcdConn.Get(timeoutCtx, ServiceRootPath+indexName+"/total-shards", etcdv3.WithPrefix())
	if getErr == nil {
		if resp != nil {
			if resp.Count > 0 {
				totalGroups, _ := strconv.Atoi(string(resp.Kvs[0].Value))
				return totalGroups
			}
		}
	}

	return 0
}

// getGroupIndex 通过哈希选择一个group
func (sentinel *Sentinel) getGroupIndex(key string) int {
	return int(farm.Hash32WithSeed([]byte(key), uint32(sentinel.seed)) % uint32(sentinel.getGroupCount()))
}
