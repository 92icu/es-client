package eslib

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"
)

func init() {
	Init("http://10.0.12.211:9200,http://10.0.12.222:9200", "elastic", "123456")
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

func TestSearchReq_Search(t *testing.T) {
	req := &SearchReq{
		Index:      "knowledge",
		SearchKey:  "掉帧",
		SortFields: map[string]string{"sid": "desc"},
		FilterField: FilterField{
			FilterTerms: map[string]string{"type": "0"},
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

func TestBase_GetTemplate(t *testing.T) {
	template, err := GetTemplate("test_template")
	if err != nil {
		panic(err)
	}
	fmt.Println(template)
}
