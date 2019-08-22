package signle_test

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	eslib "gitlab.blackshark.com/golibs/esclient"
	"testing"
)

func TestTermSuggest(t *testing.T) {
	suggest := elastic.NewTermSuggester("temp").Text("Johh").Field("name")

	result, err := eslib.Client.Search(index).Suggester(suggest).Do(context.Background())
	if err != nil {
		panic(err)
	}
	for _, suggest := range result.Suggest["temp"] {
		for _, opt := range suggest.Options {
			fmt.Println(opt.Text)
		}
	}
}

func TestPhraseSuggest(t *testing.T) {
	suggest := elastic.NewPhraseSuggester("temp").Text("Johh").Field("name")

	result, err := eslib.Client.Search(index).Suggester(suggest).Do(context.Background())
	if err != nil {
		panic(err)
	}
	for _, suggest := range result.Suggest["temp"] {
		for _, opt := range suggest.Options {
			fmt.Println(opt.Text)
		}
	}
}

func TestCompletionSuggest(t *testing.T) {
	suggest := elastic.NewCompletionSuggester("temp").Text("失败").Field("motto.suggest_text").Analyzer("ik_max_word")

	result, err := eslib.Client.Search(index).Suggester(suggest).Do(context.Background())
	if err != nil {
		panic(err)
	}
	for _, suggest := range result.Suggest["temp"] {
		for _, opt := range suggest.Options {
			fmt.Println(opt.Text)
		}
	}
}

func ContextSuggest(index, field, text string) []elastic.SearchSuggestion {
	query := elastic.NewSuggesterCategoryQuery("suggest_text", text)
	suggest := elastic.NewContextSuggester("temp").ContextQuery(query).Field(field)

	result, err := eslib.Client.Search(index).Suggester(suggest).Do(context.Background())
	if err != nil {
		panic(err)
	}
	return result.Suggest["temp"]
}
