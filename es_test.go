package eslib

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic/v7"
	"log"
	"testing"
	"time"
)

func init() {
	//测试环境
	Init("http://10.0.12.211:9200,http://10.0.12.212:9200,http://10.0.12.222:9200", "elastic", "elastic")
}

type Search struct {
	SID       int
	CID       int
	Title     string
	Keywords  string
	Author    string
	AuthorID  int
	Content   string
	Type      int //1 问题 2知识 3
	Model     string
	CreatedAt time.Time
}

func TestBase_PutMapping(t *testing.T) {
	mapping := `{"properties":{"content":{"type":"text","analyzer":"ik_max_word","search_analyzer":"ik_smart"}}}`

	if _, err := PutMapping("test", mapping); err != nil {
		panic(err)
	}
}

func TestBase_GetMapping(t *testing.T) {
	mapping, err := GetMapping("test")
	if err != nil {
		panic(err)
	}
	log.Printf(mapping)
}

func TestBase_DeleteIndex(t *testing.T) {
	if _, err := DeleteIndex("test"); err != nil {
		panic(err)
	}

}

func TestBase_GetTemplate(t *testing.T) {
	template, err := GetTemplate("test_template")
	if err != nil {
		panic(err)
	}
	fmt.Println(template)
}

func TestUpsert(t *testing.T) {
	data := map[string]interface{}{
		"name":    "Jack",
		"sex":     "男",
		"subject": "语文",
		"score":   88,
		"date":    time.Now(),
	}
	status, _ := Upsert("test", "", data)
	fmt.Println(status)
}

func TestBulk(t *testing.T) {
	data := map[string]interface{}{
		"name":     "Jack",
		"sex":      "男",
		"subject":  "语文",
		"score":    88,
		"date":     time.Now(),
		"interest": []string{"音乐", "美食"},
	}
	datas := []interface{}{data}
	err := Bulk("test", datas)
	fmt.Println(err)
}

func TestSearchReq_Search(t *testing.T) {
	req := &CommonSearch{
		Index:      "knowledge",
		SearchKey:  "掉帧",
		SortFields: map[string]string{"sid": "desc"},
		Filters: []*CommonFilter{
			{
				FilterType:  FILTER_TYPE_RANGE,
				FilterField: "type",
				FilterValue: []interface{}{0, 0},
			},
		},
		Page:     1,
		PageSize: 10,
	}

	total, hits, err := req.Search()
	if err != nil {
		panic(err)
	}
	fmt.Println("total: ", total)

	for _, hit := range hits {
		var v *Search
		if err := json.Unmarshal(hit.Source, &v); err != nil {
			panic(err)
		}

		fmt.Printf("%v\n\n", v)
	}
}

func TestMetricAgg_AggsMetric(t *testing.T) {
	ma := &CommonAggregate{
		AggName:  "stats_age",
		AggField: "age",
		AggType:  AGG_TYPE_VALUECOUNT,
	}
	value, _ := ma.AggsCommon()
	fmt.Println(value)
}

func TestBucketAgg_AggsBucket(t *testing.T) {
	ba := &CommonAggregate{
		Index:    "test",
		AggName:  "bkt_sex",
		AggField: "sex.keyword",
		AggType:  AGG_BUCKET_TERM,
		AggSize:  10,
	}
	value, err := ba.AggsCommon()
	if err != nil {
		log.Println(err)
	}
	fmt.Println(value)
}

func TestFilterAggregate_AggsFilter(t *testing.T) {
	filter := &FilterAggregate{
		CommonFilter: &CommonFilter{
			FilterType:  FILTER_TYPE_TERM,
			FilterField: "sex.keyword",
			FilterValue: []interface{}{"男", "女"},
		},
		CommonAggregate: &CommonAggregate{
			Index:    "test",
			AggName:  "filter_score",
			AggField: "score",
			AggType:  AGG_TYPE_AVG,
		},
	}

	value, err := filter.AggsFilter()
	if err != nil {
		log.Println(err)
	}
	fmt.Println(value)
}

func TestFiltersAggregate_AggsFilters(t *testing.T) {
	filter := &FiltersAggregate{
		Filters: []*CommonFilter{
			{
				FilterName:  "agg_sex",
				FilterType:  FILTER_TYPE_TERM,
				FilterField: "sex.keyword",
				FilterValue: []interface{}{"男"},
			},
			{
				FilterName:  "agg_score",
				FilterType:  FILTER_TYPE_RANGE,
				FilterField: "score",
				FilterValue: []interface{}{90},
			},
		},
		CommonAggregate: &CommonAggregate{
			Index:    "test",
			AggName:  "avg_score",
			AggField: "score",
			AggType:  AGG_TYPE_AVG,
		},
	}

	value, err := filter.AggsFilters()
	if err != nil {
		log.Println(err)
	}
	fmt.Println(value)
}

func TestDateHistAggregate_AggsDateHist(t *testing.T) {
	da := &DateHistAggregate{
		Index:    "test",
		Interval: "30s", // milliseconds(ms) , seconds(s), minutes(m), hours(h), days(d),week(w, 1w), month(M, 1M), quarter(q, 1q), year(y, 1y)
		Field:    "date",
		Format:   "yyyy-MM-dd HH:mm:ss",
		TimeZone: "Asia/Shanghai",
	}

	value, err := da.AggsDateHist()
	if err != nil {
		log.Fatal(value)
	}
	fmt.Println(value)
}

// 组装子聚合查询
func TestSubAggregate(t *testing.T) {
	agg_sub := elastic.NewStatsAggregation().Field("score")

	aggs := elastic.NewTermsAggregation().Field("subject.keyword")
	// 添加子聚合
	aggs.SubAggregation("agg_sub", agg_sub)

	result, err := Client.Search("test").Aggregation("temp", aggs).Size(0).Do(context.Background())
	if err != nil {
		log.Println(err)
	}
	var value map[string]interface{}
	json.Unmarshal(result.Aggregations["temp"], &value)
	fmt.Println(value)
}

func TestSuggest(t *testing.T) {
	template := `{
"index_patterns" : [
      "blogs*"
    ],
"settings": {
  "analysis": {
    "tokenizer" : {
      "my_pinyin" : {
          "type" : "pinyin",
          "keep_separate_first_letter" : true,
          "keep_full_pinyin" : true,
          "keep_original" : true,
          "limit_first_letter_length" : 16,
          "lowercase" : true,
          "remove_duplicated_term" : true
      }
    },
    "analyzer": {
      "pinyin_analyzer": {
        "tokenizer": "my_pinyin"
      }
    }
  }
},
"mappings": {
    "properties": {
      "body": {
        "type": "text",
        "analyzer": "ik_max_word",
        "fields": {
          "keyword": {
            "type": "keyword"
          },
          "suggest_text": {
            "type": "completion",
            "analyzer": "standard",
            "preserve_separators": false
          },
          "pinyin": {
            "type": "completion",
            "analyzer": "pinyin_analyzer",
            "preserve_separators": false
          }
        }
      }
    }
  }
}`
	PutTemplate("blog_template", template)

	Upsert("blogs", "", `{"body": "黑鲨游戏手机 2"}`)
	Upsert("blogs", "", `{"body": "黑鲨科技有限公司"}`)
	Upsert("blogs", "", `{"body": "南昌黑鲨科技"}`)
	Upsert("blogs", "", `{"body": "游戏手机哪家强"}`)
	Upsert("blogs", "", `{"body": "玩游戏用黑鲨"}`)
}

func TestDeleteTemplate(t *testing.T) {
	_, err := Client.IndexDeleteTemplate("blog_template").Do(context.Background())
	if err != nil {
		panic(err)
	}
}

func TestTermSuggest(t *testing.T) {
	suggests := TermSuggest("blogs", "body.pinyin", "hs")

	for _, suggest := range suggests {
		for _, opt := range suggest.Options {
			fmt.Println(opt.Text)
		}
	}
}

func TestPhraseSuggest(t *testing.T) {
	suggests := PhraseSuggest("blogs", "body.suggest_text", "黑鲨")

	for _, suggest := range suggests {
		for _, opt := range suggest.Options {
			fmt.Println(opt.Text)
		}
	}
}

func TestCompletionSuggest(t *testing.T) {
	suggests := CompletionSuggest("blogs", "body.suggest_text", "黑鲨")

	for _, suggest := range suggests {
		for _, opt := range suggest.Options {
			fmt.Println(opt.Text)
		}
	}
}

func TestContextSuggest(t *testing.T) {
	suggests := ContextSuggest("blogs", "body.suggest_text", "黑鲨")
	for _, suggest := range suggests {
		for _, opt := range suggest.Options {
			fmt.Println(opt.Text)
		}
	}
}
