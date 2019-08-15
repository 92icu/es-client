package eslib

import (
	"context"
	"github.com/olivere/elastic/v7"
	"log"
	"strings"
)

type HightLight struct {
	HighlightFields   []string // 列表字段匹配到了关键字则高亮返回，匹配的字词用 HighlightPostTags，HighlightPreTags包裹
	HighlightPostTags string
	HighlightPreTags  string
}

// 搜索请求参数
type CommonSearch struct {
	Index      string             `json:"Index" validate:"required"` // es 索引
	SearchKey  string             // 模糊搜索词
	FieldBoost map[string]float64 // 搜索限定字段及权重, 为空时搜索所有字段，权重默认为 1.0
	Analyzer   string             // 默认 standard
	SortFields map[string]string  // 排序 field -> desc/asc
	Page       int                `json:"Page" validate:"gt=0"`
	PageSize   int                `json:"PageSize" validate:"gt=0"`
	Filters    []*CommonFilter
	*HightLight
}

func (r *CommonSearch) Search() (total int64, hits []*elastic.SearchHit, err error) {
	if err = validate.Struct(r); err != nil {
		return
	}

	boolQuery := r.getBoolQuery()

	search := Client.Search(r.Index).Query(boolQuery)

	if sorters := getSorters(r.SortFields); sorters != nil {
		search.SortBy(sorters...)
	}
	if highlight := getHighlight(r.HightLight); highlight != nil {
		search.Highlight(highlight)
	}

	offset := (r.Page - 1) * r.PageSize
	resp, err := search.From(offset).Size(r.PageSize).Do(context.Background())

	if err != nil {
		return
	}
	return resp.TotalHits(), resp.Hits.Hits, nil
}

func (r *CommonSearch) getBoolQuery() *elastic.BoolQuery {
	boolQuery := elastic.NewBoolQuery()

	if match := getMatch(r.SearchKey, r.Analyzer, r.FieldBoost); match != nil {
		boolQuery.Must(match)
	}
	if filters := getFilters(r.Filters); filters != nil {
		boolQuery.Filter(filters...)
	}
	return boolQuery
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
func getFilters(filters []*CommonFilter) []elastic.Query {
	if filters == nil || len(filters) <= 0 {
		return nil
	}
	var querys = make([]elastic.Query, 0)
	for _, fl := range filters {
		filter := getFilter(fl)
		if filter == nil {
			continue
		}
		querys = append(querys, filter)
	}
	return querys
}

func getFilter(filter *CommonFilter) elastic.Query {
	if len(filter.FilterValue) <= 0 {
		log.Printf("filterField[%s] - filterValue is null.", filter.FilterField)
		return nil
	}
	switch filter.FilterType {
	case FILTER_TYPE_TERM:
		return elastic.NewTermsQuery(filter.FilterField, filter.FilterValue...)
	case FILTER_TYPE_RANGE:
		rangeQuery := elastic.NewRangeQuery(filter.FilterField)
		if len(filter.FilterValue) == 1 {
			rangeQuery.Gte(filter.FilterValue[0])
		} else if len(filter.FilterValue) == 2 {
			rangeQuery.Gte(filter.FilterValue[0]).Lte(filter.FilterValue[1])
		}
		return rangeQuery
	}
	return nil
}

// 高亮设置
func getHighlight(hightlight *HightLight) *elastic.Highlight {
	if hightlight == nil {
		return nil
	}
	hlfs := make([]*elastic.HighlighterField, 0)
	for _, f := range hightlight.HighlightFields {
		hl := elastic.NewHighlighterField(f)
		hlfs = append(hlfs, hl)
	}
	hl := elastic.NewHighlight().Fields(hlfs...).
		PreTags(hightlight.HighlightPreTags).PostTags(hightlight.HighlightPostTags)
	return hl
}
