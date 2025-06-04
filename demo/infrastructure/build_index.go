package infrastructure

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/WlayRay/ElectricSearch/service"
	"github.com/WlayRay/ElectricSearch/types"
	"github.com/WlayRay/ElectricSearch/util"
	"github.com/dgryski/go-farm"
	"github.com/gogo/protobuf/proto"
)

func BuildIndexFromCSVFile(csvFile string, indexer service.IIndexer, totalShards, currentGroup int) {
	file, err := os.Open(csvFile)
	if err != nil {
		log.Printf("open file %s failed, err: %v", csvFile, err)
		return
	}
	defer file.Close()

	loc, _ := time.LoadLocation("Asia/Shanghai")
	reader := csv.NewReader(file)
	reader.Comma = '|'       // 设置分隔符为竖线
	reader.LazyQuotes = true // 允许不匹配的引号
	progress := 0
	for {
		record, err := reader.Read()
		if err != nil {
			if err != io.EOF {
				log.Printf("read file %s failed, err: %v", csvFile, err)
			}
			break
		}

		if len(record) < 10 {
			continue
		}
		docId := strings.TrimPrefix(record[0], "https://www.bilibili.com/video/")

		if totalShards > 0 && int(farm.Hash32WithSeed([]byte(docId), 0))%totalShards != currentGroup {
			continue
		}

		video := &BiliBiliVideo{
			Id:     docId,
			Title:  record[1],
			Author: record[3],
		}

		if len(record[2]) > 4 {
			t, err := time.ParseInLocation("2006-01-02 15:04:05", record[2], loc)
			if err != nil {
				log.Printf("parse record \n%v\n failed record len: %d, in %d line, err: %v", record, len(record), progress+1, err)
			} else {
				video.PostTime = t.Unix()
			}
		}

		n, _ := strconv.Atoi(record[4])
		video.ViewCount = int32(n)
		n, _ = strconv.Atoi(record[5])
		video.LikeCount = int32(n)
		n, _ = strconv.Atoi(record[6])
		video.CoinCount = int32(n)
		n, _ = strconv.Atoi(record[7])
		video.FavoriteCount = int32(n)
		n, _ = strconv.Atoi(record[8])
		video.ShareCount = int32(n)
		keywords := strings.Split(record[9], ",")
		if len(keywords) > 0 {
			for _, keyword := range keywords {
				keyword = strings.TrimSpace(keyword)
				if len(keyword) > 0 {
					video.Keywords = append(video.Keywords, strings.ToLower(keyword))
				}
			}
		}
		AddVideoToIndex(video, indexer)
		progress++
		// util.Log.Printf("add %d documents to index currently", progress)
	}
	util.Log.Info("add %d documents to index totally", progress)
}

func AddVideoToIndex(video *BiliBiliVideo, indexer service.IIndexer) {
	doc := types.Document{Id: video.Id}
	bs, err := proto.Marshal(video)

	if err == nil {
		doc.Bytes = bs
	} else {
		log.Printf("serialize video %s failed, err: %v", video.Id, err)
		return
	}

	keywords := make([]*types.Keyword, 0, len(video.Keywords))
	for _, keyword := range video.Keywords {
		keywords = append(keywords, &types.Keyword{Field: "content", Word: strings.ToLower(keyword)})
	}

	if len(video.Author) > 0 {
		keywords = append(keywords, &types.Keyword{Field: "author", Word: strings.ToLower(video.Author)})
	}
	doc.Keywords = keywords
	doc.BitsFeature = GetCategoriesBits(video.Keywords)

	indexer.AddDoc(doc)
}
