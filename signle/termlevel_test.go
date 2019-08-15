package signle

import (
	"context"
	"github.com/olivere/elastic/v7"
	eslib "gitlab.blackshark.com/golibs/esclient"
	"testing"
)

func init() {
	eslib.Init("http://10.0.12.211:9200,http://10.0.12.212:9200,http://10.0.12.222:9200", "elastic", "elastic")
}

func TestExist(t *testing.T) {
	query := elastic.NewExistsQuery("body")
	result, err := eslib.Client.Search("blogs").Query(query).Do(context.Background())
	printResult(result, err)
}

func TestFuzzy(t *testing.T) {
	query := elastic.NewFuzzyQuery("body", "fxo")
	result, err := eslib.Client.Search("blogs").Query(query).Do(context.Background())
	printResult(result, err)
}

func TestPrefix(t *testing.T) {
	query := elastic.NewPrefixQuery("body.keyword", "brow")
	result, err := eslib.Client.Search("blogs").Query(query).Do(context.Background())
	printResult(result, err)
}
