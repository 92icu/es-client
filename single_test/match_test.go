package signle_test

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	eslib "gitlab.blackshark.com/golibs/esclient"
	"reflect"
	"testing"
)

func init() {
	//测试环境
	InitES()
}

//匹配，模糊匹配，
func TestMatch(t *testing.T) {
	// Operator: 默认 or
	match := elastic.NewMatchQuery("motto.pinyin", "sr").Operator("and")
	//模糊查询
	//match := elastic.NewMatchQuery("motto", "Courtisy Go").Fuzziness("AUTO:3,6").Operator("or")
	result, err := eslib.Client.Search(index).Query(match).Do(context.Background())
	printResult(result, err)
}

//短语匹配
func TestMatchPhrase(t *testing.T) {
	match := elastic.NewMatchPhraseQuery("motto.ik", "成")
	//短语前缀匹配，英文支持
	//match := elastic.NewMatchPhrasePrefixQuery("motto", "costs nothing Cour")
	result, err := eslib.Client.Search(index).Query(match).Do(context.Background())
	printResult(result, err)
}

//多字段匹配
func TestMultiMatch(t *testing.T) {
	//Type can be "best_fields", "boolean", "most_fields", "cross_fields", "phrase", or "phrase_prefix".
	//best_fields: 每个字段匹配，取 _score 评分最高多字段
	//most_fields: 每个字段匹配, _score 评分取平均
	//cross_fields: 组合字段后匹配
	match := elastic.NewMultiMatchQuery("数学", "motto", "subject").Type("best_fields")
	result, err := eslib.Client.Search(index).Query(match).Do(context.Background())
	printResult(result, err)
}

//字符串查询
func TestQueryString(t *testing.T) {
	//query := elastic.NewQueryStringQuery("失败是*母").AnalyzeWildcard(true)
	query := elastic.NewSimpleQueryStringQuery("失败是*母").AnalyzeWildcard(true)
	result, err := eslib.Client.Search(index).Query(query).Do(context.Background())
	printResult(result, err)
}

func printResult(result *elastic.SearchResult, err error) {
	if err != nil {
		panic(err)
	}
	fmt.Println("total: ", result.TotalHits())
	each := result.Each(reflect.TypeOf(Student{}))
	fmt.Println(each)
}
