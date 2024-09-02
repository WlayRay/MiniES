package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/WlayRay/ElectricSearch/v1.0.0/internal/kvdb"
	"github.com/WlayRay/ElectricSearch/v1.0.0/types"
	"github.com/WlayRay/ElectricSearch/v1.0.0/util"
)

const (
	INDEX_SERVICE = "index_service"
)

// IndexServiceWorker是一个grpc服务，用于索引文档
type IndexServiceWorker struct {
	Indexer  *Indexer
	hub      *ServiceHub
	selfAddr string
}

func (service *IndexServiceWorker) Init(workerIndex ...int) error {
	service.Indexer = new(Indexer)

	var docNumEstimate, dbType int
	var dbPath string
	if v, ok := util.Configurations["document-estimate-num"]; ok {
		docNumEstimate, _ = strconv.Atoi(v)
	} else {
		docNumEstimate = 50000
	}
	if v, ok := util.Configurations["db-path"]; ok {
		dbPath = util.RootPath + strings.Replace(v, "\"", "", -1)
		if dbPath[len(dbPath)-1] != '/' {
			dbPath += "/"
		}
		if v, ok := util.Configurations["db-type"]; ok {
			switch v {
			case "badger":
				dbType = kvdb.BADGER
				dbPath += "badger_db"
			default:
				dbType = kvdb.BOLT
				dbPath += "bolt_db/bolt"
			}
		} else {
			dbType = kvdb.BOLT
		}
		util.Log.Println("db path:", dbPath)
		if workerIndex != nil {
			dbPath += "_" + strconv.Itoa(workerIndex[0])
		}
	}
	return service.Indexer.Init(docNumEstimate, dbType, dbPath)
}

func (service *IndexServiceWorker) Regist(etcdEndpoint []string, servicePort, heartRate int) error {
	// 向注册中心注册自己
	if len(etcdEndpoint) > 0 {
		if servicePort < 1024 {
			return fmt.Errorf("invalid listen port %d, should more than 1024", servicePort)
		}
		selfLocalIp, err := util.GetLocalIP()
		selfLocalIp = "127.0.0.1" // 仅在本机模拟分布式部署用
		if err != nil {
			panic(err)
		}
		service.selfAddr = fmt.Sprintf("%s:%d", selfLocalIp, servicePort)
		hub := GetServiceHub(etcdEndpoint, int64(heartRate))
		leaseId, err := hub.Regist(INDEX_SERVICE, service.selfAddr, 0)
		if err != nil {
			panic(err)
		}
		service.hub = hub
		go func() {
			for {
				hub.Regist(INDEX_SERVICE, service.selfAddr, leaseId)
				time.Sleep(time.Duration(heartRate)*time.Second - 100*time.Millisecond)
			}
		}()
	}
	return nil
}

// 向索引中添加文档，如果文档已存在则会覆盖
func (service *IndexServiceWorker) AddDoc(ctx context.Context, doc *types.Document) (*AffectedCount, error) {
	n, err := service.Indexer.AddDoc(*doc)
	return &AffectedCount{uint32(n)}, err
}

// 从索引上删除文档
func (service *IndexServiceWorker) DeleteDoc(ctx context.Context, docId *DocId) (*AffectedCount, error) {
	n := service.Indexer.DeleteDoc(docId.DocId)
	return &AffectedCount{uint32(n)}, nil
}

// 检索，返回文档列表
func (service *IndexServiceWorker) Search(ctx context.Context, request *SearchRequest) (*SearchResponse, error) {
	documents := service.Indexer.Search(request.Query, request.OnFlag, request.OffFlag, request.OrFlags)
	return &SearchResponse{Documents: documents}, nil
}

func (service *IndexServiceWorker) Count(ctx context.Context, request *CountRequest) (*AffectedCount, error) {
	n := service.Indexer.Count()
	return &AffectedCount{Count: uint32(n)}, nil
}

func (service *IndexServiceWorker) Close() {
	if service.hub != nil {
		service.hub.Close()
	}
}
