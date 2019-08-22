package signle_test

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	eslib "gitlab.blackshark.com/golibs/esclient"
	"testing"
)

const index = "student"

func TestAggsTerms(t *testing.T) {
	agg := elastic.NewTermsAggregation().Field("subject")
	result, err := eslib.Client.Search(index).Aggregation("temp", agg).Size(0).Do(context.Background())
	if err != nil {
		panic(err)
	}
	items, _ := result.Aggregations.Terms("temp")
	for _, item := range items.Buckets {
		fmt.Println(item.Key, item.DocCount)
	}
}

func TestAggsFilter(t *testing.T) {
	query := elastic.NewTermQuery("name", "John1")
	agg := elastic.NewAvgAggregation().Field("score")
	filter := elastic.NewFilterAggregation().Filter(query).SubAggregation("sub_agg", agg)
	result, err := eslib.Client.Search(index).Aggregation("temp", filter).Size(0).Do(context.Background())
	if err != nil {
		fmt.Println("search error")
		panic(err)
	}
	bucket, _ := result.Aggregations.Filters("temp")
	metric, _ := bucket.Aggregations.Avg("sub_agg")
	fmt.Println(*metric.Value)
}

func TestAggsFilters(t *testing.T) {
	query := elastic.NewTermQuery("name", "John1")
	query2 := elastic.NewTermQuery("name", "John2")
	agg := elastic.NewAvgAggregation().Field("score")
	filters := elastic.NewFiltersAggregation().
		FilterWithName("filter1", query).
		FilterWithName("filter2", query2).
		SubAggregation("sub_agg", agg)
	result, err := eslib.Client.Search(index).Aggregation("temp", filters).Size(0).Do(context.Background())
	if err != nil {
		fmt.Println("search error")
		panic(err)
	}
	bucket, _ := result.Aggregations.Filters("temp")
	filter1 := bucket.NamedBuckets["filter1"]
	metric, _ := filter1.Aggregations.Avg("sub_agg")
	fmt.Println(*metric.Value)
	filter2 := bucket.NamedBuckets["filter2"]
	metric2, _ := filter2.Aggregations.Avg("sub_agg")
	fmt.Println(*metric2.Value)
}

func TestAggsHistogram(t *testing.T) {
	agg := elastic.NewHistogramAggregation().Field("score").Interval(30)
	result, err := eslib.Client.Search(index).Aggregation("temp", agg).Size(0).Do(context.Background())
	if err != nil {
		panic(err)
	}
	items, _ := result.Aggregations.Histogram("temp")
	for _, item := range items.Buckets {
		fmt.Println(item.Key, item.DocCount)
	}
}

func TestAggsRanges(t *testing.T) {
	agg := elastic.NewRangeAggregation().Field("score").AddRange(0, 90).AddRange(91, 120).AddRange(121, 150)
	result, err := eslib.Client.Search(index).Aggregation("temp", agg).Size(0).Do(context.TODO())
	if err != nil {
		panic(err)
	}
	items, _ := result.Aggregations.Range("temp")
	for _, item := range items.Buckets {
		fmt.Println(item.Key, item.DocCount)
	}
}

func TestAggsGeoDistance(t *testing.T) {
	agg := elastic.NewGeoDistanceAggregation().Field("location").
		Point("31.191570,121.523288").
		AddRange(nil, 100).AddRange(101, 500).AddRange(501, nil)
	result, err := eslib.Client.Search(index).Aggregation("temp", agg).Size(0).Do(context.TODO())
	if err != nil {
		panic(err)
	}
	items, _ := result.Aggregations.GeoDistance("temp")
	for _, item := range items.Buckets {
		fmt.Println(item.Key, item.DocCount)
	}
}

func TestAggsGeoHash(t *testing.T) {
	agg := elastic.NewGeoHashGridAggregation().Field("location").Precision(5)
	result, err := eslib.Client.Search(index).Aggregation("temp", agg).Do(context.TODO())
	if err != nil {
		panic(err)
	}
	items, _ := result.Aggregations.GeoHash("temp")
	for _, item := range items.Buckets {
		fmt.Println(item.Key, item.DocCount)
	}
}
