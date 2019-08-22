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
	InitES()
}

//匹配词高亮
func TestHighLight(t *testing.T) {
	query := elastic.NewMatchQuery("motto.ik", "成功")
	hl := elastic.NewHighlight().Field("motto.ik").PreTags("<h1>").PostTags("</h1>")
	result, err := eslib.Client.Search(index).Query(query).Highlight(hl).Do(context.Background())
	if err != nil {
		panic(err)
	}
	each := result.Each(reflect.TypeOf(Student{}))
	fmt.Println(each)
	for _, hit := range result.Hits.Hits {
		fmt.Println(hit.Highlight)
	}
}
