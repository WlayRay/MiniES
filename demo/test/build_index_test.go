package test

import (
	"os"
	"testing"

	"github.com/WlayRay/ElectricSearch/config"
	infrastructure "github.com/WlayRay/ElectricSearch/demo/infrastructure"
	"github.com/WlayRay/ElectricSearch/internal/kvdb"
	"github.com/WlayRay/ElectricSearch/service"
)

var (
	dbType  = kvdb.BADGER
	dbPath  = config.RootPath + "data/local_db/video_bolt"
	indexer *service.Indexer
)

func Init() {
	os.Remove(dbPath) //x先删除原有的索引文件
	indexer = new(service.Indexer)
	if err := indexer.Init(50000, dbType, dbPath); err != nil {
		panic(err)
	}
}

func TestBuildIndex(t *testing.T) {
	Init()
	defer indexer.Close()
	csvFile := config.RootPath + "data/bilibili_video.csv" // 改成项目中实际的csv文件路径
	infrastructure.BuildIndexFromCSVFile(csvFile, indexer, 0, 0)
}
