package eslib

import (
	"context"
	"github.com/olivere/elastic/v7"
	"strings"
)

type HightLight struct {
	HighlightFields   []string // 列表字段匹配到了关键字则高亮返回，匹配的字词用 HighlightPostTags，HighlightPreTags包裹
	HighlightPostTags string
	HighlightPreTags  string
}

// 搜索请求参数
type SearchReq struct {
	Index      string             `json:"Index" validate:"required"` // es 索引
	SearchKey  string             // 模糊搜索词
	FieldBoost map[string]float64 // 搜索限定字段及权重, 为空时搜索所有字段，权重默认为 1.0
	Analyzer   string             // 默认 standard
	SortFields map[string]string  // 排序 field -> desc/asc
	Page       int                `json:"Page" validate:"gt=0"`
	PageSize   int                `json:"PageSize" validate:"gt=0"`
	FilterField
	HightLight
}

func (r *SearchReq) Search() (total int64, hits []*elastic.SearchHit, err error) {
	if err = validate.Struct(r); err != nil {
		return
	}

	boolQuery := elastic.NewBoolQuery()

	if match := getMatch(r.SearchKey, r.Analyzer, r.FieldBoost); match != nil {
		boolQuery.Must(match)
	}
	if filters := getFilters(r.FilterTerms, r.FilterRange); filters != nil {
		boolQuery.Filter(filters...)
	}

	search := client.Search(r.Index).Query(boolQuery)

	if sorters := getSorters(r.SortFields); sorters != nil {
		search.SortBy(sorters...)
	}
	if highlight := getHighlight(r.HighlightFields, r.HighlightPreTags, r.HighlightPostTags); highlight != nil {
		search.Highlight(highlight)
	}

	offset := (r.Page - 1) * r.PageSize
	resp, err := search.From(offset).Size(r.PageSize).Do(context.Background())

	if err != nil {
		return
	}
	return resp.TotalHits(), resp.Hits.Hits, nil
}

// 模糊匹配
func getMatch(searchKey string, analyzer string, fieldBoost map[string]float64) elastic.Query {
	if fieldBoost == nil || len(fieldBoost) <= 0 {
		return nil
	}

	match := elastic.NewMultiMatchQuery(searchKey)
	// 字段查询权重设置
	for f, b := range fieldBoost {
		match.FieldWithBoost(f, b)
	}
	if analyzer != "" && len(analyzer) > 0 {
		match.Analyzer(analyzer)
	}
	return match
}

// 排序设置
func getSorters(sortFields map[string]string) []elastic.Sorter {
	if sortFields == nil || len(sortFields) <= 0 {
		return nil
	}
	sorters := make([]elastic.Sorter, 0)
	for f, s := range sortFields {
		fs := elastic.NewFieldSort(f)
		if strings.ToLower(s) == "desc" {
			fs.Desc()
		} else {
			fs.Asc()
		}
		sorters = append(sorters, fs)
	}
	return sorters
}

// 过滤条件
func getFilters(terms, rangeField map[string]string) []elastic.Query {
	if (terms == nil || len(terms) <= 0) && (rangeField == nil || len(rangeField) <= 0) {
		return nil
	}
	// 精确匹配 过滤
	var filters = make([]elastic.Query, 0)
	if terms != nil && len(terms) > 0 {
		for f, b := range terms {
			filters = append(filters, elastic.NewTermQuery(f, b))
		}
	}

	//范围匹配
	if rangeField != nil && len(rangeField) > 0 {
		for f, b := range rangeField {
			rq := elastic.NewRangeQuery(f)

			nums := strings.Split(b, ",")
			if len(nums) <= 0 || len(nums) > 2 {
				continue
			} else {
				if len(nums[0]) > 0 {
					rq.Gte(nums[0])
				}
				if len(nums) == 2 && len(nums[1]) > 0 {
					rq.Lte(nums[1])
				}
			}
			filters = append(filters, rq)
		}
	}
	return filters
}

// 高亮设置
func getHighlight(highlightFields []string, preTags, postTags string) *elastic.Highlight {
	if highlightFields == nil || len(highlightFields) <= 0 {
		return nil
	}
	hlfs := make([]*elastic.HighlighterField, 0)
	for _, f := range highlightFields {
		hl := elastic.NewHighlighterField(f)
		hlfs = append(hlfs, hl)
	}
	hl := elastic.NewHighlight().Fields(hlfs...).
		PreTags(preTags).PostTags(postTags)
	return hl
}
