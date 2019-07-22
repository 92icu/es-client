package eslib

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/olivere/elastic/v7"
	"log"
)

type Base struct {
	Index        string `json:"indices" validate:"required"`
	Id           string
	Mapping      string
	TemplateName string
	Template     string
}

type FilterField struct {
	FilterTerms map[string]string // 过滤条件, 精确匹配 field -> value
	FilterRange map[string]string // 过滤条件, 范围搜索, 数值及时间范围 两个值用逗号隔开, 时间格式化 yyyy-MM-dd HH:mm:ss
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
func (b *Base) PutMapping() (ok bool, err error) {
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
	if b.Mapping == "" {
		return false, errors.New("Not set mapping ")
	}

	resp, err := client.PutMapping().Index(b.Index).BodyString(b.Mapping).Do(context.Background())
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

func (b *Base) PutTemplate() (ok bool, err error) {
	if err = validate.Struct(b); err != nil {
		return
	}

	if b.TemplateName == "" {
		return false, errors.New("Not set template name! ")
	}
	if b.Template == "" {
		return false, errors.New("Not set template content! ")
	}

	resp, err := client.IndexPutTemplate(b.TemplateName).BodyString(b.Template).Do(context.Background())
	if err != nil {
		return
	}
	return resp.Acknowledged, err
}

func (b *Base) GetTemplate() (template string, err error) {
	if b.TemplateName == "" {
		return "", errors.New("Not set template-name! ")
	}
	responses, err := client.IndexGetTemplate(b.TemplateName).Do(context.Background())
	if err != nil {
		return
	}
	bytes, err := json.Marshal(responses[b.TemplateName])
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
	if elastic.IsNotFound(err) {
		log.Printf("id[%s] is not exist!", b.Id)
		return ok, nil
	}
	if err != nil {
		return
	}
	return del.Status, nil
}

// 按照条件删除数据
func (b *Base) DeleteWithQuery(filters *FilterField) (failedId []string, err error) {
	if filters == nil {
		return
	}
	query := elastic.NewBoolQuery()
	if fl := getFilters(filters.FilterTerms, filters.FilterRange); fl != nil {
		query.Filter(fl...)
	}

	resp, err := client.DeleteByQuery(b.Index).Query(query).Do(context.Background())
	if err != nil {
		return
	}
	failedId = make([]string, 0)
	for _, failure := range resp.Failures {
		failedId = append(failedId, failure.Id)
	}
	return
}
