package signle_test

import (
	eslib "gitlab.blackshark.com/golibs/esclient"
	"testing"
	"time"
)

func InitES() {
	urls := "http://10.0.12.211:9200,http://10.0.12.212:9200,http://10.0.12.222:9200"
	if err := eslib.Init(urls, "elastic", "elastic"); err != nil {
		panic(err)
	}
}

func addStudent() {
	eslib.DeleteIndex("student")
	eslib.DeleteTemplate("student")
	mapping := `{
"index_patterns" : [
      "student*"
    ],
"settings": {
  "number_of_shards": 1,
  "number_of_replicas": 1,
  "refresh_interval": "30s",
  "analysis": {
    "tokenizer" : {
      "my_pinyin" : {
          "type" : "pinyin",
          "keep_separate_first_letter" : true,
          "keep_full_pinyin" : true,
          "keep_original" : true,
          "limit_first_letter_length" : 16,
          "lowercase" : true,
          "remove_duplicated_term" : true
      }
    },
    "analyzer": {
      "pinyin_analyzer": {
        "tokenizer": "my_pinyin"
      }
    }
  }
},
"mappings": {
  "properties": {
     "name": {
       "type": "keyword"
     },
     "subject": {
       "type": "keyword"
     },
     "score": {
       "type": "float"
     },
	  "score_weight": {
       "type": "float"
     },
	  "birthday": {
       "type": "date"
     },
     "interest": {
       "type": "text",
       "analyzer": "ik_max_word"
     },
     "motto": {
       "type": "text",
       "fields": {
          "ik": {
            "type": "text",
			"analyzer": "ik_max_word"
          },
          "suggest_text": {
            "type": "completion",
            "analyzer": "standard",
            "preserve_separators": false
          },
          "pinyin": {
            "type": "text",
            "store": false,
			"term_vector": "with_offsets",
			"analyzer": "pinyin_analyzer",
			"boost": 10
          }
        }
     }, 
     "location": {
        "type": "geo_point"
     }
   }
}}`
	if _, err := eslib.PutTemplate("student", mapping); err != nil {
		panic(err)
	}
	stus := []interface{}{
		&Student{
			Name:        "John1",
			Subject:     "数学",
			Score:       85.5,
			ScoreWeight: 1.5,
			Motto:       "虽然过去不能改变，未来可以。",
			Interest:    []string{"音乐", "篮球", "旅游"},
			Location:    "31.191570,121.523288",
			Birthday:    time.Date(2000, 1, 12, 0, 0, 0, 0, time.Local),
		},
		&Student{
			Name:        "John1",
			Subject:     "语文",
			Score:       108,
			ScoreWeight: 1.5,
			Motto:       "虽然过去不能改变，未来可以。",
			Interest:    []string{"音乐", "篮球", "旅游"},
			Location:    "31.191570,121.523288",
			Birthday:    time.Date(2000, 1, 12, 0, 0, 0, 0, time.Local),
		},
		&Student{
			Name:        "John2",
			Subject:     "数学",
			Score:       98,
			ScoreWeight: 1.5,
			Motto:       "不求做的最好，但求做的更好。",
			Interest:    []string{"音乐", "摄影", "旅游"},
			Location:    "31.191643,121.527826",
			Birthday:    time.Date(2000, 1, 12, 0, 0, 0, 0, time.Local),
		},
		&Student{
			Name:        "John2",
			Subject:     "语文",
			Score:       114,
			ScoreWeight: 1.5,
			Motto:       "不求做的最好，但求做的更好。",
			Interest:    []string{"音乐", "摄影", "旅游"},
			Location:    "31.191643,121.527826",
			Birthday:    time.Date(1999, 10, 12, 0, 0, 0, 0, time.Local),
		},
		&Student{
			Name:     "John3",
			Subject:  "数学",
			Score:    68,
			Motto:    "失败是成功之母！",
			Interest: []string{"羽毛球", "篮球", "电影"},
			Location: "31.190303,121.523835",
			Birthday: time.Date(2000, 3, 22, 0, 0, 0, 0, time.Local),
		},
		&Student{
			Name:     "John4",
			Subject:  "数学",
			Score:    47,
			Motto:    "Courtesy costs nothing",
			Interest: []string{"摄影", "篮球", "电影"},
			Location: "31.203575,121.557634",
			Birthday: time.Date(2001, 11, 22, 0, 0, 0, 0, time.Local),
		},
		&Student{
			Name:     "John5",
			Subject:  "语文",
			Score:    99,
			Motto:    "costs nothing Courtesy Go",
			Interest: []string{"摄影", "篮球", "电影"},
			Location: "31.142267,121.808682",
			Birthday: time.Date(2001, 11, 22, 0, 0, 0, 0, time.Local),
		},
	}
	if err := eslib.Bulk("student", stus); err != nil {
		panic(err)
	}

}

func TestInitData(t *testing.T) {
	InitES()
	addStudent()
}
