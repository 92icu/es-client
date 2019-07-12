package eslib

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"log"
)

type Base struct {
	Index    string `json:"indices" validate:"required"`
	Id       string
	Mapping  string
	Template string
}

func (b *Base) ExistIndex() (exist bool, err error) {
	exist, err = client.IndexExists(b.Index).Do(context.Background())
	return
}

func (b *Base) CreateIndex() (err error) {
	if _, err = client.CreateIndex(b.Index).Pretty(true).Do(context.Background()); err != nil {
		return
	}
	log.Printf("index[%s] create successed.", b.Index)
	return
}

// 新增/更新 mapping，index 不存在时自动创建
func (b *Base) PutMapping(mapping string) (ok bool, err error) {
	if err = validate.Struct(b); err != nil {
		return
	}
	exist, err := b.ExistIndex()
	if err != nil {
		return
	}
	if !exist {
		if err = b.CreateIndex(); err != nil {
			return
		}
	}

	resp, err := client.PutMapping().Index(b.Index).BodyString(mapping).Do(context.Background())
	if err != nil {
		return
	}
	return resp.Acknowledged, nil
}

func (b *Base) GetMapping() (mapping string, err error) {
	data, err := client.GetMapping().Index(b.Index).Do(context.Background())
	if err != nil {
		return
	}
	bytes, err := json.Marshal(data)
	return string(bytes), err
}

// 删除索引
func (b *Base) DeleteIndex() (ok bool, err error) {
	exist, err := b.ExistIndex()
	if err != nil {
		return
	}
	if !exist {
		log.Printf("index[%s] is not exists.", b.Index)
		return
	}
	resp, err := client.DeleteIndex(b.Index).Pretty(true).Do(context.Background())
	if err != nil {
		return
	}
	return resp.Acknowledged, nil
}

// ID不存在则新增，存在则更新
func (b *Base) Upsert(data interface{}) (status int, err error) {
	ret, err := client.Index().Index(b.Index).Id(b.Id).BodyJson(data).Do(context.Background())
	if err != nil {
		return
	}
	return ret.Status, nil
}

// 批量新增
func (b *Base) Bulk(datas []interface{}) error {
	bulk := client.Bulk()
	for _, data := range datas {
		doc := elastic.NewBulkUpdateRequest().Index(b.Index).Doc(data)
		bulk.Add(doc)
	}
	_, err := bulk.Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

// 自定义 id，datas: id -> [data]
func (b *Base) BulkWithId(datas map[string]interface{}) error {
	bulk := client.Bulk()
	for id, data := range datas {
		doc := elastic.NewBulkUpdateRequest().Index(b.Index).Id(id).Doc(data)
		bulk.Add(doc)
	}
	_, err := bulk.Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

// 删除指定 ID 数据
func (b *Base) DeleteById() (ok int, err error) {
	del, err := client.Delete().Index(b.Index).Id(b.Id).Do(context.TODO())
	if err != nil {
		return
	}
	return del.Status, nil
}
