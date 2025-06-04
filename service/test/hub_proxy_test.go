package servicetest

import (
	"fmt"

	"github.com/WlayRay/ElectricSearch/service"

	// "fmt"
	"testing"
)

func TestGetServiceEndpointByProxy(t *testing.T) {
	var (
		qps         = 10
		etcdServers = []string{"127.0.0.1:2379"}
		Endpoints   = []string{"127.0.0.1:6080", "127.0.0.2:6081", "127.0.0.3:6082"}
		group       = "group-1"
	)

	proxy := service.GetServiceHubProxy(etcdServers, 30, qps)

	_ = proxy.Register(group, Endpoints[0])
	_ = proxy.Register(group, Endpoints[1])
	_ = proxy.Register(group, Endpoints[2])
	defer func() {
		if err := proxy.UnRegister(group, Endpoints[0]); err != nil {
			fmt.Printf("unregister %s failed: %v\n", Endpoints[0], err)
		}
	}()
	defer func() {
		if err := proxy.UnRegister(group, Endpoints[1]); err != nil {
			fmt.Printf("unregister %s failed: %v\n", Endpoints[1], err)
		}
	}()
	defer func() {
		if err := proxy.UnRegister(group, Endpoints[2]); err != nil {
			fmt.Printf("unregister %s failed: %v\n", Endpoints[2], err)
		}
	}()

	groupWorkers := proxy.GetServiceEndpoints(group)
	for i := 0; i < 3; i++ {
		oneWorker := proxy.GetServiceEndpoint(group)
		fmt.Printf("%d endpoints: %v\n", len(groupWorkers), oneWorker)
	}

	//time.Sleep(1 * time.Second)
	//for i := 0; i < qps+5; i++ { // 桶里只有10个令牌，后五次访问会失败
	//	endpoints := proxy.GetServiceEndpoints(group)
	//	fmt.Printf("%d endpoints: %v\n", len(Endpoints), endpoints)
	//}
	//
	//time.Sleep(1 * time.Second) // 暂停一秒使令牌容量恢复
	//for i := 0; i < qps+5; i++ {
	//	endpoints := proxy.GetServiceEndpoints(group)
	//	fmt.Printf("%d endpoints: %v\n", len(Endpoints), endpoints)
	//}
}
