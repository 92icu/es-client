package eslib

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"
)

func init() {
	//Init("http://106.52.30.252:9200", "elastic", "elastic")
	Init("http://10.0.12.211:9200,http://10.0.12.212:9200,http://10.0.12.222:9200", "elastic", "123456")
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
		"name":    "Jame",
		"sex":     "女",
		"subject": "数学",
		"score":   98,
		"date":    time.Now(),
	}
	status, _ := Upsert("test", "", data)
	fmt.Println(status)
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
