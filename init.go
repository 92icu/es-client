package eslib

import (
	"github.com/olivere/elastic/v7"
	"gopkg.in/go-playground/validator.v9"
	"strings"
)

var client *elastic.Client
var validate *validator.Validate

func Init(hosts, username, password string) (err error) {
	validate = validator.New()
	urls := strings.Split(hosts, ",")
	client, err = elastic.NewClient(elastic.SetURL(urls...),
		elastic.SetBasicAuth(username, password),
		elastic.SetSniff(false))
	return
}

func GetClient(hosts, username, password string) (cli *elastic.Client, err error) {
	err = Init(hosts, username, password)
	return client, err
}
