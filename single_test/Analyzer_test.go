package signle_test

import (
	"context"
	"fmt"
	eslib "gitlab.blackshark.com/golibs/esclient"
	"testing"
)

func init() {
	InitES()
}

func TestAnalyzer(t *testing.T) {
	response, err := eslib.Client.IndexAnalyze().Analyzer("ik_max_word").Text("中国是世界上人数最多的国家").Do(context.Background())
	//response, err := eslib.Client.IndexAnalyze().Tokenizer("ik_max_word").Text("中国是世界上人数最多的国家").Do(context.Background())

	if err != nil {
		panic(err)
	}
	fmt.Println(response)
}
