package signle_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic/v7"
	eslib "gitlab.blackshark.com/golibs/esclient"
	"testing"
	"time"
)

type Student struct {
	Name        string    `json:"name"`
	Subject     string    `json:"subject"`
	Score       float64   `json:"score"`
	ScoreWeight float64   `json:"score_weight"`
	Birthday    time.Time `json:"birthday"`
	Interest    []string  `json:"interest"`
	Motto       string    `json:"motto"`
	Location    string    `json:"location"`
}

func init() {
	InitES()
}

func TestAggsStats(t *testing.T) {
	//agg := elastic.NewAvgAggregation().Field("score")
	//agg := elastic.NewWeightedAvgAggregation().
	//	Value(&elastic.MultiValuesSourceFieldConfig{FieldName: "score"}).
	//	Weight(&elastic.MultiValuesSourceFieldConfig{FieldName: "score_weight", Missing: 2})
	//agg := elastic.NewMaxAggregation().Field("score")
	//agg := elastic.NewMinAggregation().Field("score")
	//agg := elastic.NewSumAggregation().Field("score")
	agg := elastic.NewStatsAggregation().Field("score")
	query := elastic.NewTermQuery("subject", "语文")
	result, err := eslib.Client.Search(index).Query(query).Aggregation("temp", agg).Do(context.Background())
	if err != nil {
		panic(err)
	}
	metric, _ := result.Aggregations.Avg("temp")
	fmt.Printf("%v", metric)
}

//去重统计
func TestAggsCardinality(t *testing.T) {
	agg := elastic.NewCardinalityAggregation().Field("subject")
	result, err := eslib.Client.Search(index).Aggregation("temp", agg).Do(context.Background())
	if err != nil {
		panic(err)
	}
	metric, _ := result.Aggregations.Cardinality("temp")
	fmt.Println(*metric.Value)
}

//地理边界框 左上/右下
func TestAggsGeoBound(t *testing.T) {
	agg := elastic.NewGeoBoundsAggregation().Field("location")
	result, err := eslib.Client.Search(index).Aggregation("temp", agg).Do(context.Background())
	if err != nil {
		panic(err)
	}
	metric, _ := result.Aggregations.GeoBounds("temp")
	fmt.Println(metric.Bounds)
}

//地理质心
func TestAggsGeoCentroid(t *testing.T) {
	agg := elastic.NewGeoCentroidAggregation().Field("location")
	result, err := eslib.Client.Search("geo_test").Aggregation("temp", agg).Do(context.Background())
	if err != nil {
		panic(err)
	}
	metric, _ := result.Aggregations.GeoCentroid("temp")
	fmt.Println(metric.Location)
}

//百分位聚合：第p百分位数是这样一个值，它使得至少有p％的数据项小于或等于这个值，且至少有(100－p)％的数据项大于或等于这个值。
func TestAggsPercentiles(t *testing.T) {
	agg := elastic.NewPercentilesAggregation().Field("score").Percentiles(1, 5, 25, 50, 75, 95, 99)
	result, err := eslib.Client.Search("student").Aggregation("temp", agg).Do(context.Background())
	if err != nil {
		panic(err)
	}
	metric, _ := result.Aggregations.Percentiles("temp")
	fmt.Println(metric.Values)
}

func TestAggsPercentilesRanks(t *testing.T) {
	agg := elastic.NewPercentileRanksAggregation().Field("score").Values(80, 100, 120)
	result, err := eslib.Client.Search("student").Aggregation("temp", agg).Do(context.Background())
	if err != nil {
		panic(err)
	}
	metric, _ := result.Aggregations.PercentileRanks("temp")
	fmt.Println(metric.Values)
}

//文档数统计
func TestAggsValueCount(t *testing.T) {
	agg := elastic.NewValueCountAggregation().Field("score_weight")
	result, _ := eslib.Client.Search("student").Size(0).Aggregation("temp", agg).Do(context.Background())
	metric, _ := result.Aggregations.ValueCount("temp")
	fmt.Println(*metric.Value)
}

func TestAggsTopHits(t *testing.T) {
	th := elastic.NewTopHitsAggregation().Sort("score", false).FetchSource(true).Size(2)
	term := elastic.NewTermsAggregation().Field("name").SubAggregation("top_aggs", th).Size(3)
	result, err := eslib.Client.Search("student").Size(0).Aggregation("temp", term).Do(context.Background())
	if err != nil {
		panic(err)
	}
	terms, _ := result.Aggregations.Terms("temp")
	for _, dt := range terms.Buckets {
		fmt.Println(dt.DocCount)
		metric, _ := dt.Aggregations.TopHits("top_aggs")
		hits := metric.Hits.Hits
		for _, hit := range hits {
			var stu Student
			json.Unmarshal(hit.Source, &stu)
			fmt.Println(stu)
		}
	}
}
