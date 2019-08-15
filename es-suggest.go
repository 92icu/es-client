package eslib

import (
	"context"
	"github.com/olivere/elastic/v7"
)

func TermSuggest(index, field, text string) []elastic.SearchSuggestion {
	suggest := elastic.NewTermSuggester("temp").Text(text).Field(field)

	result, err := Client.Search(index).Suggester(suggest).Do(context.Background())
	if err != nil {
		panic(err)
	}
	return result.Suggest["temp"]
}

func PhraseSuggest(index, field, text string) []elastic.SearchSuggestion {
	suggest := elastic.NewPhraseSuggester("temp").Text(text).Field(field)

	result, err := Client.Search(index).Suggester(suggest).Do(context.Background())
	if err != nil {
		panic(err)
	}
	return result.Suggest["temp"]
}

func CompletionSuggest(index, field, text string) []elastic.SearchSuggestion {
	suggest := elastic.NewCompletionSuggester("temp").Text(text).Field(field).Analyzer("ik_max_word")

	result, err := Client.Search(index).Suggester(suggest).Do(context.Background())
	if err != nil {
		panic(err)
	}
	return result.Suggest["temp"]
}

func ContextSuggest(index, field, text string) []elastic.SearchSuggestion {
	query := elastic.NewSuggesterCategoryQuery("suggest_text", text)
	suggest := elastic.NewContextSuggester("temp").ContextQuery(query).Field(field)

	result, err := Client.Search(index).Suggester(suggest).Do(context.Background())
	if err != nil {
		panic(err)
	}
	return result.Suggest["temp"]
}
