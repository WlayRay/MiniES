package servicetest

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/WlayRay/ElectricSearch/service"
	"github.com/WlayRay/ElectricSearch/types"
	"github.com/WlayRay/ElectricSearch/util"

	"google.golang.org/grpc"
)

var (
	workPorts   = []int{6689} //在一台机器上启多个worker，实际中是一台机器上启一个worker
	etcdServers = []string{"127.0.0.1:2379"}
)

func StartWorkers() (closeFns []func() error) {
	for _, port := range workPorts {
		// 监听本地端口
		lis, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err != nil {
			panic(err)
		}

		server := grpc.NewServer()
		indexServiceWorker := new(service.IndexServiceWorker)
		if err = indexServiceWorker.Init(etcdServers, 0, 3); err != nil {
			panic(err)
		}
		indexServiceWorker.Indexer.LoadFromIndexFile() //从文件中加载索引数据
		// 注册服务的具体实现
		service.RegisterIndexServiceServer(server, indexServiceWorker)
		if err = indexServiceWorker.Register(port); err != nil {
			panic(err)
		}
		go func(port int) {
			// 启动服务
			util.Log.Info("start grpc server on port %d\n", port)
			//Serve会一直阻塞，所以放到一个协程里异步执行
			if err = server.Serve(lis); err != nil {
				_ = indexServiceWorker.Close()
				util.Log.Error("start grpc server on port %d failed: %s\n", port, err)
			}
		}(port)
		closeFns = append(closeFns, func() error {
			return indexServiceWorker.Close() // 关闭索引服务
		})
	}
	return
}

func TestIndexCluster(t *testing.T) {
	closeFns := StartWorkers()
	defer func() {
		if r := recover(); r != nil {
			util.Log.Warn("recovered from panic: %v", r)
			for _, closeFn := range closeFns {
				if err := closeFn(); err != nil {
					util.Log.Error("close worker failed: %s", err)
				} else {
					util.Log.Info("worker closed successfully")
				}
			}
		}
	}()
	defer func() {
		for _, closeFn := range closeFns {
			if err := closeFn(); err != nil {
				util.Log.Error("close worker failed: %s", err)
			} else {
				util.Log.Info("worker closed successfully")
			}
		}
	}()
	time.Sleep(3 * time.Second) //等所有worker都启动完毕

	sentinel := service.NewSentinel(etcdServers)
	//测试Add接口
	book := Book{
		ISBN:    "436246383",
		Title:   "上下五千年",
		Author:  "李四",
		Price:   39.0,
		Content: "冰雪奇缘2 中文版电影原声带 Frozen 2 Mandarin Original Motion Picture",
	}
	doc := types.Document{
		Id:          book.ISBN,
		BitsFeature: 0b10011, //二进制
		Keywords:    []*types.Keyword{{Field: "content", Word: "唐朝"}, {Field: "content", Word: "文物"}, {Field: "title", Word: book.Title}},
		Bytes:       book.Serialize(),
	}
	_, err := sentinel.AddDoc(doc)
	if err != nil {
		util.Log.Error("添加失败: %s", err)
		t.Fail()
	} else {
		count := sentinel.Count() // 获取当前索引的文档总数
		util.Log.Debug("当前doc总数%d\n", count)
	}
	//测试Search接口
	query := types.NewTermQuery("content", "文物")
	query = query.And(types.NewTermQuery("content", "唐朝"))
	docs := sentinel.Search(query, 0, 0, nil)
	if err != nil {
		util.Log.Error("检索失败: %s", err)
		t.Fail()
	} else {
		docId := ""
		if len(docs) == 0 {
			util.Log.Warn("无搜索结果")
		} else {
			for _, doc := range docs {
				book := DeserializeBook(doc.Bytes) //检索的结果是二进流，需要自反序列化
				if book != nil {
					util.Log.Debug("%s %s %s %s %.1f\n", doc.Id, book.ISBN, book.Title, book.Author, book.Price)
					docId = doc.Id
				}
			}
		}
		//测试Delete接口
		if len(docId) > 0 {
			_ = sentinel.DeleteDoc(docId)
		}

		//测试Search接口
		docs := sentinel.Search(query, 0, 0, nil)
		count := sentinel.Count() // 获取当前索引的文档总数
		if len(docs) == 0 {
			util.Log.Debug("当前文档总数：%d，无搜索结果", count)
		} else {
			for _, doc := range docs {
				book := DeserializeBook(doc.Bytes) //检索的结果是二进流，需要自反序列化
				if book != nil {
					util.Log.Debug("%s %s %s %s %.1f\n", doc.Id, book.ISBN, book.Title, book.Author, book.Price)
				}
			}
		}
	}
}
