package eslib

import (
	"context"
	"github.com/olivere/elastic/v7"
	"gopkg.in/go-playground/validator.v9"
	"strings"
)

var client *elastic.Client

func Init(hosts, username, password string) (err error) {
	urls := strings.Split(hosts, ",")
	client, err = elastic.NewClient(elastic.SetURL(urls...),
		elastic.SetBasicAuth(username, password),
		elastic.SetSniff(false))
	return
}

func GetClient(hosts, username, password string) (cli *elastic.Client, err error) {
	err = Init(hosts, username, password)
	return client, err
}

// 搜索请求参数
type ESSearchReq struct {
	Index             string             `json:"Index" validate:"required"` // es 索引
	ID                string             //ID
	SearchKey         string             // 模糊搜索词
	Terms             map[string]string  // 过滤条件,精确匹配， field -> value
	FieldBoost        map[string]float64 // 搜索限定字段及权重, 为空时搜索所有字段，权重默认为 1.0
	RangeField        map[string]string  // 范围搜索，包括时间范围
	HighlightFields   []string
	HighlightPostTags string
	HighlightPreTags  string
	Analyzer          string            // 默认 standard
	SortFields        map[string]string //排序 field -> desc/asc
	Page              int               `json:"Page" validate:"gt=0"`
	PageSize          int               `json:"PageSize" validate:"gt=0"`
}

// ID不存在则新增，存在则更新
func (r *ESSearchReq) Upsert(data interface{}) (ok int, err error) {
	var ret *elastic.IndexResponse
	if len(r.ID) == 0 {
		ret, err = client.Index().Index(r.Index).BodyJson(data).Do(context.Background())
	} else {
		ret, err = client.Index().Index(r.Index).Id(r.ID).BodyJson(data).Do(context.Background())
	}
	if err != nil {
		return
	}
	return ret.Status, nil
}

// 批量新增
func (r *ESSearchReq) Bulk(datas []interface{}) error {
	bulk := client.Bulk()
	for _, data := range datas {
		doc := elastic.NewBulkUpdateRequest().Index(r.Index).Doc(data)
		bulk.Add(doc)
	}
	_, err := bulk.Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

// 自定义 id，datas: id -> [data]
func (r *ESSearchReq) BulkWithId(datas map[string]interface{}) error {
	bulk := client.Bulk()
	for id, data := range datas {
		doc := elastic.NewBulkUpdateRequest().Index(r.Index).Id(id).Doc(data)
		bulk.Add(doc)
	}
	_, err := bulk.Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

// 删除指定 ID 数据
func (r *ESSearchReq) Delete() (ok int, err error) {
	del, err := client.Delete().Index(r.Index).Id(r.ID).Do(context.TODO())
	if err != nil {
		return
	}
	return del.Status, nil
}

func (r *ESSearchReq) Search() (total int64, hits []*elastic.SearchHit, err error) {
	validate := validator.New()
	if err = validate.Struct(r); err != nil {
		return
	}

	boolQuery := elastic.NewBoolQuery()

	match := getMatch(r.SearchKey, r.FieldBoost)
	filters := getFilters(r.Terms, r.RangeField)
	sorters := getSorters(r.SortFields)
	highlight := getHighlight(r.HighlightFields, r.HighlightPreTags, r.HighlightPostTags)

	boolQuery.Must(match).Filter(filters...)

	resp, err := client.Search(r.Index).
		Query(boolQuery).
		SortBy(sorters...).
		Highlight(highlight).
		Do(context.Background())

	if err != nil {
		return
	}
	return resp.TotalHits(), resp.Hits.Hits, nil
}

// 模糊匹配
func getMatch(searchKey string, fieldBoost map[string]float64) elastic.Query {
	match := elastic.NewMultiMatchQuery(searchKey)

	// 字段查询权重设置
	if fieldBoost != nil && len(fieldBoost) > 0 {
		for f, b := range fieldBoost {
			match.FieldWithBoost(f, b)
		}
	}
	return match
}

// 排序设置
func getSorters(sortFields map[string]string) []elastic.Sorter {
	sorters := make([]elastic.Sorter, 0)
	if sortFields != nil && len(sortFields) > 0 {
		for f, s := range sortFields {
			fs := elastic.NewFieldSort(f)
			if strings.ToLower(s) == "desc" {
				fs.Desc()
			} else {
				fs.Asc()
			}
			sorters = append(sorters, fs)
		}
	}
	return sorters
}

// 过滤条件
func getFilters(terms, rangeField map[string]string) []elastic.Query {
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

			nums := strings.Split(b, ":")
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
	hlfs := make([]*elastic.HighlighterField, 0)
	if highlightFields != nil && len(highlightFields) > 0 {
		for _, f := range highlightFields {
			hl := elastic.NewHighlighterField(f)
			hlfs = append(hlfs, hl)
		}
	}
	hl := elastic.NewHighlight().Fields(hlfs...).
		PreTags(preTags).PostTags(postTags)
	return hl
}
