package eslib

import (
	"github.com/olivere/elastic/v7"
	"gopkg.in/go-playground/validator.v9"
	"strings"
)

var client *elastic.Client
var validate *validator.Validate

func Init(urls, username, password string) (err error) {
	validate = validator.New()
	hosts := strings.Split(urls, ",")
	client, err = elastic.NewClient(elastic.SetURL(hosts...),
		elastic.SetBasicAuth(username, password),
		elastic.SetSniff(false))
	return
}

func GetClient(urls, username, password string) (cli *elastic.Client, err error) {
	err = Init(urls, username, password)
	return client, err
}
