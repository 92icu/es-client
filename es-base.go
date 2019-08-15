package eslib

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/olivere/elastic/v7"
	"log"
)

type filterType int

const (
	FILTER_TYPE_TERM filterType = iota
	FILTER_TYPE_RANGE
)

type CommonFilter struct {
	FilterType  filterType
	FilterName  string
	FilterField string
	FilterValue []interface{}
}

func ExistIndex(index string) (exist bool, err error) {
	exist, err = Client.IndexExists(index).Do(context.Background())
	return
}

func CreateIndex(index string) (err error) {
	if _, err = Client.CreateIndex(index).Pretty(true).Do(context.Background()); err != nil {
		return
	}
	log.Printf("index[%s] create successed.", index)
	return
}

// 新增/更新 mapping，index 不存在时自动创建
func PutMapping(index, mapping string) (ok bool, err error) {
	exist, err := ExistIndex(index)
	if err != nil {
		return
	}
	if !exist {
		if err = CreateIndex(index); err != nil {
			return
		}
	}
	if mapping == "" {
		return false, errors.New("Not set mapping ")
	}

	resp, err := Client.PutMapping().Index(index).BodyString(mapping).Do(context.Background())
	if err != nil {
		return
	}
	return resp.Acknowledged, nil
}

func GetMapping(index string) (mapping string, err error) {
	data, err := Client.GetMapping().Index(index).Do(context.Background())
	if err != nil {
		return
	}
	bytes, err := json.Marshal(data)
	return string(bytes), err
}

func PutTemplate(tplName, template string) (ok bool, err error) {
	if tplName == "" {
		return false, errors.New("Not set template name! ")
	}
	if template == "" {
		return false, errors.New("Not set template content! ")
	}

	resp, err := Client.IndexPutTemplate(tplName).BodyString(template).Do(context.Background())
	if err != nil {
		return
	}
	return resp.Acknowledged, err
}

func GetTemplate(tplName string) (template string, err error) {
	if tplName == "" {
		return "", errors.New("Not set template-name! ")
	}
	responses, err := Client.IndexGetTemplate(tplName).Do(context.Background())
	if err != nil {
		return
	}
	bytes, err := json.Marshal(responses[tplName])
	return string(bytes), err
}

// 删除索引
func DeleteIndex(index string) (ok bool, err error) {
	exist, err := ExistIndex(index)
	if err != nil {
		return
	}
	if !exist {
		log.Printf("index[%s] is not exists.", index)
		return
	}
	resp, err := Client.DeleteIndex(index).Pretty(true).Do(context.Background())
	if err != nil {
		return
	}
	return resp.Acknowledged, nil
}

// ID不存在则新增，存在则更新
func Upsert(index string, id string, data interface{}) (status int, err error) {
	ret, err := Client.Index().Index(index).Id(id).BodyJson(data).Do(context.Background())
	if err != nil {
		return
	}
	return ret.Status, nil
}

// 批量新增
func Bulk(index string, datas []interface{}) error {
	bulk := Client.Bulk()
	for _, data := range datas {
		doc := elastic.NewBulkIndexRequest().Index(index).Doc(data)
		bulk.Add(doc)
	}
	_, err := bulk.Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

// 自定义 id，datas: id -> [data]
func BulkWithId(index string, datas map[string]interface{}) error {
	bulk := Client.Bulk()
	for id, data := range datas {
		doc := elastic.NewBulkUpdateRequest().Index(index).Id(id).Doc(data)
		bulk.Add(doc)
	}
	_, err := bulk.Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

// 删除指定 ID 数据
func DeleteById(index, id string) (ok int, err error) {
	del, err := Client.Delete().Index(index).Id(id).Do(context.TODO())
	if elastic.IsNotFound(err) {
		log.Printf("id[%s] is not exist!", id)
		return ok, nil
	}
	if err != nil {
		return
	}
	return del.Status, nil
}

// 按照条件删除数据
func DeleteWithQuery(index string, filters []*CommonFilter) (failedId []string, err error) {
	if filters == nil {
		return
	}
	query := elastic.NewBoolQuery()
	if fl := getFilters(filters); fl != nil {
		query.Filter(fl...)
	}

	resp, err := Client.DeleteByQuery(index).Query(query).Do(context.Background())
	if err != nil {
		return
	}
	failedId = make([]string, 0)
	for _, failure := range resp.Failures {
		failedId = append(failedId, failure.Id)
	}
	return
}
