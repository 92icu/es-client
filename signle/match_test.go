package signle

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
	eslib.Init("http://10.0.12.211:9200,http://10.0.12.212:9200,http://10.0.12.222:9200", "elastic", "elastic")
}

//匹配，模糊匹配，
func TestMatch(t *testing.T) {
	//match := elastic.NewMatchQuery("body", "黑鲨公司").Operator("AND")
	//模糊查询
	match := elastic.NewMatchQuery("body", "fox").Fuzziness("AUTO:3,6")
	result, err := eslib.Client.Search("blogs").Query(match).Do(context.Background())
	printResult(result, err)
}

//短语匹配
func TestMatchPhrase(t *testing.T) {
	//match := elastic.NewMatchPhraseQuery("body", "游戏手机")
	//短语前缀匹配，
	match := elastic.NewMatchPhrasePrefixQuery("body", "游戏手机哪")
	result, err := eslib.Client.Search("blogs").Query(match).Do(context.Background())
	printResult(result, err)
}

//多字段匹配
func TestMultiMatch(t *testing.T) {
	//Type can be "best_fields", "boolean", "most_fields", "cross_fields", "phrase", or "phrase_prefix".
	//best_fields: 每个字段匹配，取 _score 评分最高多字段
	//most_fields: 每个字段匹配, _score 评分取平均
	//cross_fields: 组合字段后匹配
	match := elastic.NewMultiMatchQuery("游戏手机哪", "body", "message").Type("phrase_prefix")
	result, err := eslib.Client.Search("blogs").Query(match).Do(context.Background())
	printResult(result, err)
}

//字符串查询
func TestQueryString(t *testing.T) {
	//query := elastic.NewQueryStringQuery("游戏手机*强").AnalyzeWildcard(true)
	query := elastic.NewSimpleQueryStringQuery("游戏手机*强").AnalyzeWildcard(true)
	result, err := eslib.Client.Search("blogs").Query(query).Do(context.Background())
	printResult(result, err)
}

type Blogs struct {
	Body string
}

func printResult(result *elastic.SearchResult, err error) {
	if err != nil {
		panic(err)
	}
	fmt.Println("total: ", result.TotalHits())
	each := result.Each(reflect.TypeOf(Blogs{}))
	fmt.Println(each)
}
