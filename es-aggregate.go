package eslib

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/olivere/elastic/v7"
)

type aggregateType int

const (
	TEMP_AGG_NAME                      = "temp_agg"
	AGG_TYPE_AVG         aggregateType = iota //求平均值
	AGG_TYPE_MIN                              //求最小值
	AGG_TYPE_MAX                              //
	AGG_TYPE_SUM                              //
	AGG_TYPE_STATS                            //统计值，包括 avg,min,max,sum,count
	AGG_TYPE_CARDINALITY                      //去重数量
	AGG_TYPE_PERCENT                          //
	AGG_TYPE_VALUECOUNT                       //文档数
	AGG_BUCKET_TERM                           //
)

type CommonAggregate struct {
	Index    string
	AggName  string
	AggField string
	AggType  aggregateType
	AggSize  int // BUCKET 类型可以设置，默认 10
}

type FilterAggregate struct {
	*CommonAggregate
	*CommonFilter
}

type FiltersAggregate struct {
	*CommonAggregate
	Filters []*CommonFilter
}

type DateHistAggregate struct {
	Index    string
	Field    string
	Type     string
	Interval string
	Format   string
	TimeZone string
	Offset   string
}

// 简单聚合
func (c *CommonAggregate) AggsCommon() (value map[string]interface{}, err error) {
	aggs := getAggregation(c)
	if aggs == nil {
		err = errors.New("错误的参数类型！")
		return
	}

	result, err := client.Search(c.Index).Aggregation(c.AggName, aggs).Size(0).Do(context.Background())
	if err != nil {
		return
	}
	messages := result.Aggregations[c.AggName]
	err = json.Unmarshal(messages, &value)
	return
}

// 按照匹配的数据聚合
func (f *FilterAggregate) AggsFilter() (value map[string]interface{}, err error) {
	filter := getFilter(f.CommonFilter)
	if filter == nil {
		return
	}
	sub_aggs := getAggregation(f.CommonAggregate)

	aggs := elastic.NewFilterAggregation().Filter(filter).SubAggregation(f.AggName, sub_aggs)

	result, err := client.Search(f.Index).Aggregation(TEMP_AGG_NAME, aggs).Size(0).Do(context.Background())
	if err != nil {
		return
	}
	messages := result.Aggregations[TEMP_AGG_NAME]
	err = json.Unmarshal(messages, &value)
	return
}

// 按照匹配的数据聚合
func (f *FiltersAggregate) AggsFilters() (value map[string]interface{}, err error) {
	sub_aggs := getAggregation(f.CommonAggregate)

	aggs := elastic.NewFiltersAggregation().SubAggregation(f.AggName, sub_aggs)
	for _, fl := range f.Filters {
		filter := getFilter(fl)
		if filter != nil {
			aggs.FilterWithName(fl.FilterName, filter)
		}
	}

	result, err := client.Search(f.Index).Aggregation(TEMP_AGG_NAME, aggs).Size(0).Do(context.Background())
	if err != nil {
		return
	}
	messages := result.Aggregations[TEMP_AGG_NAME]
	err = json.Unmarshal(messages, &value)
	return
}

// 按时间间隔统计数据量
func (d *DateHistAggregate) AggsDateHist() (value map[string]interface{}, err error) {
	aggs := elastic.NewDateHistogramAggregation().
		Field(d.Field).Interval(d.Interval).
		Format(d.Format).TimeZone(d.TimeZone).Offset(d.Offset)

	result, err := client.Search(d.Index).Aggregation(TEMP_AGG_NAME, aggs).Size(0).Do(context.Background())
	if err != nil {
		return
	}
	messages := result.Aggregations[TEMP_AGG_NAME]
	err = json.Unmarshal(messages, &value)
	return
}

func getAggregation(c *CommonAggregate) elastic.Aggregation {
	switch c.AggType {
	case AGG_TYPE_AVG:
		return elastic.NewAvgAggregation().Field(c.AggField)
	case AGG_TYPE_MAX:
		return elastic.NewMaxAggregation().Field(c.AggField)
	case AGG_TYPE_MIN:
		return elastic.NewMinAggregation().Field(c.AggField)
	case AGG_TYPE_SUM:
		return elastic.NewSumAggregation().Field(c.AggField)
	case AGG_TYPE_STATS:
		return elastic.NewStatsAggregation().Field(c.AggField)
	case AGG_TYPE_CARDINALITY:
		return elastic.NewCardinalityAggregation().Field(c.AggField)
	case AGG_TYPE_PERCENT:
		return elastic.NewPercentilesAggregation().Field(c.AggField)
	case AGG_TYPE_VALUECOUNT:
		return elastic.NewValueCountAggregation().Field(c.AggField)
	case AGG_BUCKET_TERM:
		size := c.AggSize
		if size <= 0 {
			size = 10
		}
		return elastic.NewTermsAggregation().Field(c.AggField).Size(size).OrderByCountDesc()
	}
	return nil
}
