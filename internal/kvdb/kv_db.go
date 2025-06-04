package kvdb

import (
	"fmt"
	"os"
	"strings"

	"github.com/WlayRay/ElectricSearch/util"
)

const (
	BADGER = iota
)

// 操作各类数据库的接口
type IKeyValueDB interface {
	Open() error                              //初始化DB
	GetDbPath() string                        //获取存储数据的目录
	Set(k, v []byte) error                    //写入<key, value>
	BatchSet(keys, values [][]byte) error     //批量写入<key, value>
	Get(k []byte) ([]byte, error)             //读取key对应的value
	BatchGet(keys [][]byte) ([][]byte, error) //批量读取，注意不保证顺序
	Delete(k []byte) error                    //删除
	BatchDelete(keys [][]byte) error          //批量删除
	Has(k []byte) bool                        //判断某个key是否存在
	IterDB(fn func(k, v []byte) error) int64  //遍历数据库，返回数据的条数
	IterKey(fn func(k []byte) error) int64    //遍历数据库，返回key的条数
	Close() error                             //把内存中的数据flush到磁盘，同时释放文件锁
}

// 工厂模式，可以根据传入的dbType构建不同的数据库产品，返回产品的接口
func GetKeyValueDB(dbType int, path string) (IKeyValueDB, error) {
	paths := strings.Split(path, "/")
	parentPath := strings.Join(paths[:len(paths)-1], "/") //获取父目录

	// 打开或创建正排索引数据存放的目录
	info, err := os.Stat(parentPath)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(parentPath, os.ModePerm); err != nil {
			return nil, err
		} else {
			util.Log.Info("create dir: %s", parentPath)
		}
	} else if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", parentPath)
	} else {
		util.Log.Info("parent dir: %s", parentPath)
	}

	var db IKeyValueDB

	switch dbType {
	default: //默认使用badger
		db = new(Badger).WithDataPath(path)
	}
	err = db.Open()
	return db, err
}
