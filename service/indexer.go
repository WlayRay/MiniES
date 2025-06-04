package service

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	"github.com/WlayRay/ElectricSearch/internal/kvdb"
	reverseindex "github.com/WlayRay/ElectricSearch/internal/reverse_index"
	"github.com/WlayRay/ElectricSearch/types"
	"github.com/WlayRay/ElectricSearch/util"

	"github.com/dgryski/go-farm"
)

// 外观模式，把正排和倒排索引2个子系统封装在一起
type Indexer struct {
	forwardIndex kvdb.IKeyValueDB
	reverseIndex reverseindex.IReverseIndex
	worker       *util.Worker // 雪花算法
}

func (indexer *Indexer) Init(DocNumEstimate int, dbtype int, DataDir string) error {
	// 通过对本机IP哈希生成雪花算法的workerId，可保证workerId唯一
	ip, _ := util.GetLocalIP()
	workerId := uint64(farm.Hash64WithSeed([]byte(ip), 0)) % 1023
	worker, err := util.NewWorker(workerId)
	if err != nil {
		panic(err)
	}
	indexer.worker = worker

	db, err := kvdb.GetKeyValueDB(dbtype, fmt.Sprintf("%s/%d%d", DataDir, time.Now().Year(), time.Now().Weekday()))
	if err != nil {
		return err
	}
	indexer.forwardIndex = db
	indexer.reverseIndex = reverseindex.NewSkipListReverseIndex(DocNumEstimate)

	return nil
}

// Close 关闭正排索引存储连接
func (indexer *Indexer) Close() error {
	return indexer.forwardIndex.Close()
}

// 倒排索引存储在内存中，系统重启时从正排索引里加载数据
func (indexer *Indexer) LoadFromIndexFile() int {
	n := indexer.forwardIndex.IterDB(func(k, v []byte) error {
		reader := bytes.NewReader(v)
		decoder := gob.NewDecoder(reader)
		var doc types.Document
		err := decoder.Decode(&doc)
		if err != nil {
			util.Log.Error("Decode error: %v", err)
			return nil
		}
		indexer.reverseIndex.Add(doc)
		return err
	})
	util.Log.Debug("Load %d data from forward index: %s", n, indexer.forwardIndex.GetDbPath())
	return int(n)
}

// 向索引中添加文档，如果文档已存在则先删除
func (indexer *Indexer) AddDoc(doc types.Document) (int, error) {
	docId := strings.TrimSpace(doc.Id)
	if len(docId) == 0 {
		return 0, nil
	}
	indexer.DeleteDoc(docId) // 先从正排和倒排索引上删除文档

	doc.IntId = indexer.worker.GetId() // 使用雪花算法生成唯一自增ID

	// 写入正排索引
	var value bytes.Buffer
	encoder := gob.NewEncoder(&value)
	if err := encoder.Encode(doc); err == nil {
		_ = indexer.forwardIndex.Set([]byte(docId), value.Bytes())
	} else {
		return 0, err
	}

	// 写入倒排索引
	indexer.reverseIndex.Add(doc)
	return 1, nil
}

func (indexer *Indexer) DeleteDoc(docId string) int {
	n := 0
	forwardKey := []byte(docId)
	docBytes, err := indexer.forwardIndex.Get(forwardKey) //先读正排索引，得到IntId和Keywords
	if err == nil {
		if len(docBytes) > 0 {
			n = 1
			reader := bytes.NewReader(docBytes)
			decoder := gob.NewDecoder(reader)
			var doc types.Document
			err := decoder.Decode(&doc)
			if err != nil {
				util.Log.Error("Decode error: %v", err)
			} else {
				// 从倒排索引上删除
				for _, keyword := range doc.Keywords {
					indexer.reverseIndex.Delete(doc.IntId, keyword)
				}
			}
		}
	} else {
		//util.Log.Printf("DeleteDoc error: %v", err)
	}
	// 从正排索引上删除
	_ = indexer.forwardIndex.Delete(forwardKey)
	return n
}

// 检索，返回文档列表
func (indexer *Indexer) Search(querys *types.TermQuery, onFlag, offFlag uint64, orFlags []uint64) []*types.Document {
	docIds := indexer.reverseIndex.Search(querys, onFlag, offFlag, orFlags)
	if len(docIds) == 0 {
		return nil
	}

	keys := make([][]byte, 0, len(docIds))
	for _, docId := range docIds {
		keys = append(keys, []byte(docId))
	}
	docs, err := indexer.forwardIndex.BatchGet(keys)
	if err != nil {
		util.Log.Error("Search from forward index error: %v", err)
	}

	results := make([]*types.Document, 0, len(docs))
	reader := bytes.NewReader([]byte{})
	// var doc types.Document //千万别把接收每个文档的变量定义放在循环外
	for _, docBytes := range docs {
		if len(docBytes) > 0 {
			reader.Reset(docBytes)
			decoder := gob.NewDecoder(reader)
			var doc types.Document //一定要把接收每个文档的变量定义放在循环内
			err := decoder.Decode(&doc)
			if err != nil {
				util.Log.Error("Decode error: %v", err)
				continue
			} else {
				results = append(results, &doc)
			}
		}
	}
	return results
}

func (indexer *Indexer) Count() int {
	n := 0
	indexer.forwardIndex.IterKey(func(k []byte) error {
		n++
		return nil
	})
	return n
}
