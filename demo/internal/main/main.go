package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/WlayRay/ElectricSearch/config"
	"github.com/WlayRay/ElectricSearch/demo/handler"
	"github.com/WlayRay/ElectricSearch/internal/kvdb"
	"github.com/WlayRay/ElectricSearch/util"
	"github.com/gin-gonic/gin"
)

var (
	mode                int
	documentEstimateNum int
	dbType              int
	dbPath              string
	rebuildIndex        bool
	currentGroup        int
	csvFilePath         string
	port                int
	etcdEndpoints       []string
	heartRate           int
)

func startGin() {
	engine := gin.Default()
	gin.SetMode(gin.ReleaseMode)

	engine.Use(handler.GetUserInfo)

	engine.POST("/search", handler.SearchAll)
	engine.POST("/up_search", handler.SearchByAuthor)

	if err := engine.Run("0.0.0.0:" + "9000"); err != nil {
		util.Panic("Server failed to start:", err)
		return
	}
}

func main() {
	switch mode {
	case 1, 3:
		WebServerMain(mode)
		startGin()
	case 2:
		GrpcIndexerMain()
	}
	// 模式1为单机部署，模式2为启动分布式部署下的每个索引服务节点，相当于一个grpc server
	// 模式3为启动分布式部署下的etcd代理（Sentinel），后续的添加、搜索、删除文档等都通过代理操作
	// 在分布式部署时，需要先通过模式2启动多个索引服务节点，然后再通过模式3启动etcd代理和web server
}

func init() {
	// 配置文件校验
	// 初始化部署模式
	modeStr := os.Getenv("MODE")
	if modeStr == "" {
		// 如果环境变量为空，从 ConfigMap 中读取
		if v, ok := config.ConfigMap["mode"]; !ok {
			panic("mode not found in ConfigMap!")
		} else {
			mode, _ = strconv.Atoi(fmt.Sprintf("%v", v))
			if mode < 1 || mode > 3 {
				panic("mode invalid!")
			}
		}
	} else {
		var err error
		mode, err = strconv.Atoi(modeStr)
		if err != nil || mode < 1 || mode > 3 {
			panic("mode invalid!")
		}
	}

	// 读取 server 配置
	serverConfig, ok := config.ConfigMap["server"].(map[string]any)
	if !ok {
		panic("server configuration not found!")
	}

	portStr := os.Getenv("PORT")
	if portStr == "" {
		if v, ok := serverConfig["port"]; !ok {
			panic("port not found in ConfigMap!")
		} else {
			port, _ = strconv.Atoi(fmt.Sprintf("%v", v))
		}
	} else {
		port, _ = strconv.Atoi(portStr)
	}

	if v, ok := serverConfig["rebuild-index"]; !ok {
		panic("rebuildIndex not found in ConfigMap!")
	} else {
		switch v := v.(type) {
		case bool:
			rebuildIndex = v
		case string:
			rebuildIndex = v == "true"
		default:
			panic("rebuildIndex invalid!")
		}
	}

	// 读取 distributed 配置
	distributedConfig, ok := config.ConfigMap["distributed"].(map[string]any)
	if mode == 2 && !ok {
		panic("distributed configuration not found!")
	}

	if mode == 2 {
		if v, ok := distributedConfig["group-index"]; !ok {
			panic("currentGroup not found in ConfigMap!")
		} else {
			currentGroup, _ = strconv.Atoi(fmt.Sprintf("%v", v))
		}

		if v, ok := distributedConfig["heart-rate"]; ok {
			heartRate, _ = strconv.Atoi(fmt.Sprintf("%v", v))
		}
	}

	// 读取 index 配置
	indexConfig, ok := config.ConfigMap["index"].(map[string]any)
	if !ok {
		panic("index configuration not found!")
	}

	// 建立索引的CSV文件
	if v, ok := indexConfig["csv-file"]; !ok {
		panic("csvFilePath not found in ConfigMap!")
	} else {
		csvFilePath = config.RootPath + strings.Replace(fmt.Sprintf("%v", v), "\"", "", -1)
		util.Debug("csvFilePath: %s", csvFilePath)
	}

	// 正排索引数据存放目录
	if v, ok := indexConfig["db-path"]; !ok {
		panic("dbPath not found in ConfigMap!")
	} else {
		dbPath = config.RootPath + strings.Replace(fmt.Sprintf("%v", v), "\"", "", -1)
	}

	// 正排索引的存储引擎
	if v, ok := indexConfig["db-type"]; !ok {
		panic("dbType not found in ConfigMap!")
	} else {
		switch v.(string) {
		default:
			dbType = kvdb.BADGER
			dbPath += "badger_db"
		}
	}

	// 预估文档数量
	if v, ok := indexConfig["document-estimate-num"]; !ok {
		panic("documentEstimateNum not found in ConfigMap!")
	} else {
		documentEstimateNum, _ = strconv.Atoi(fmt.Sprintf("%v", v))
	}

	// 读取 etcd 配置
	etcdConfig, ok := config.ConfigMap["etcd"].(map[string]any)
	if !ok {
		panic("etcd configuration not found!")
	}

	if v, ok := etcdConfig["servers"]; ok {
		servers, ok := v.([]any)
		if !ok {
			panic("etcd servers configuration invalid!")
		}
		for _, server := range servers {
			endpoint := strings.TrimSpace(fmt.Sprintf("%v", server))
			endpoint = strings.Replace(endpoint, "\"", "", -1)
			etcdEndpoints = append(etcdEndpoints, endpoint)
		}
		util.Debug("etcdEndpoints: %v", etcdEndpoints)
	}
}
