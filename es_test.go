package eslib

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"
)

func init() {
	Init("http://106.52.30.252:9200", "elastic", "elastic")
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
	req := &Base{
		Index: "test",
	}
	mapping := `{"properties":{"content":{"type":"text","analyzer":"ik_max_word","search_analyzer":"ik_smart"}}}`
	if _, err := req.PutMapping(mapping); err != nil {
		panic(err)
	}
}

func TestBase_GetMapping(t *testing.T) {
	req := &Base{
		Index: "test",
	}
	mapping, err := req.GetMapping()
	if err != nil {
		panic(err)
	}
	log.Printf(mapping)
}

func TestBase_DeleteIndex(t *testing.T) {
	req := &Base{
		Index: "test",
	}
	if _, err := req.DeleteIndex(); err != nil {
		panic(err)
	}

}

func TestSearchReq_Search(t *testing.T) {
	req := &SearchReq{
		Index:      "knowledge",
		SearchKey:  "掉帧",
		SortFields: map[string]string{"sid": "desc"},
		Terms:      map[string]string{"type": "0"},
		Page:       1,
		PageSize:   10,
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
