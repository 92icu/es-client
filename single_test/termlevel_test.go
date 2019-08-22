package signle_test

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	eslib "gitlab.blackshark.com/golibs/esclient"
	"testing"
)

func init() {
	InitES()
}

//字段存在则返回
func TestExist(t *testing.T) {
	query := elastic.NewExistsQuery("name")
	result, err := eslib.Client.Search(index).Query(query).Do(context.Background())
	printResult(result, err)
}

//模糊查询
func TestFuzzy(t *testing.T) {
	query := elastic.NewFuzzyQuery("motto", "csots")
	result, err := eslib.Client.Search(index).Query(query).Do(context.Background())
	printResult(result, err)
}

//词前缀匹配
func TestPrefix(t *testing.T) {
	query := elastic.NewPrefixQuery("motto", "cost")
	result, err := eslib.Client.Search(index).Query(query).Do(context.Background())
	printResult(result, err)
}

//完全匹配
func TestTerm(t *testing.T) {
	query := elastic.NewTermQuery("subject", "数学")
	result, err := eslib.Client.Search(index).Query(query).Do(context.Background())
	printResult(result, err)
}

func TestTerms(t *testing.T) {
	query := elastic.NewTermsQuery("subject", "数数", "语文")
	result, err := eslib.Client.Search(index).Query(query).Do(context.Background())
	printResult(result, err)
}

//完全匹配, 模糊
func TestWildcard(t *testing.T) {
	query := elastic.NewWildcardQuery("subject", "数*")
	result, err := eslib.Client.Search(index).Query(query).Do(context.Background())
	printResult(result, err)
}

func TestTid(t *testing.T) {
	a := 1
	b := 2
	fmt.Println(a ^ b)
}
