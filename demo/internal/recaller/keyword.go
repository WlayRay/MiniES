package recaller

import (
	"strings"

	"github.com/WlayRay/ElectricSearch/util"

	infrastructure "github.com/WlayRay/ElectricSearch/demo/infrastructure"
	"github.com/WlayRay/ElectricSearch/types"
	"github.com/gogo/protobuf/proto"
)

type KeywordRecaller struct{}

type KeywordAuthorRecaller struct{}

func (KeywordRecaller) Recall(ctx *infrastructure.VideoSearchContext) []*infrastructure.BiliBiliVideo {
	request := ctx.Request
	if request == nil {
		return nil
	}
	indexer := ctx.Indexer
	if indexer == nil {
		return nil
	}

	keywords := request.Keywords
	query := new(types.TermQuery)
	if len(keywords) > 0 {
		for _, keyword := range keywords {
			query = query.And(types.NewTermQuery("content", keyword))
		}
	}
	if len(request.Author) > 0 {
		query = query.And(types.NewTermQuery("author", strings.ToLower(request.Author)))
	}
	orFlags := []uint64{(infrastructure.GetCategoriesBits(request.Categories))}
	docs := indexer.Search(query, 0, 0, orFlags)

	videos := make([]*infrastructure.BiliBiliVideo, 0, len(docs))
	for _, doc := range docs {
		var video infrastructure.BiliBiliVideo
		if err := proto.Unmarshal(doc.Bytes, &video); err == nil {
			videos = append(videos, &video)
		}
	}
	return videos
}

func (KeywordAuthorRecaller) Recall(ctx *infrastructure.VideoSearchContext) []*infrastructure.BiliBiliVideo {
	request := ctx.Request
	if request == nil {
		return nil
	}
	indexer := ctx.Indexer
	if indexer == nil {
		return nil
	}
	keywords := request.Keywords
	query := new(types.TermQuery)
	if len(keywords) > 0 {
		for _, keyword := range keywords {
			query.And(types.NewTermQuery("content", keyword))
		}
	}
	v := ctx.Ctx.Value("user_name")
	if v != nil {
		if author, ok := v.(string); ok {
			if len(author) > 0 {
				query = query.And(types.NewTermQuery("author", strings.ToLower(author)))
			}
		}
	}

	orFlags := []uint64{(infrastructure.GetCategoriesBits(request.Categories))}
	docs := indexer.Search(query, 0, 0, orFlags)
	videos := make([]*infrastructure.BiliBiliVideo, 0, len(docs))
	for _, doc := range docs {
		var video infrastructure.BiliBiliVideo
		if err := proto.Unmarshal(doc.Bytes, &video); err == nil {
			videos = append(videos, &video)
		} else {
			util.Error("unmarshal failed: %v", err)
		}
	}
	return videos
}
