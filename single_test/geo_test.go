package signle_test

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	eslib "gitlab.blackshark.com/golibs/esclient"
	"reflect"
	"testing"
)

const geoIndex = "student"

func init() {
	//InitData()
	InitES()
}

//地理边界框
func TestGeoBoundingBox(t *testing.T) {
	query := elastic.NewBoolQuery()
	match := elastic.NewMatchAllQuery()
	filter := elastic.NewGeoBoundingBoxQuery("location").TopLeft(31.192543, 121.522569).BottomRight(31.188578, 121.526410)
	query.Must(match).Filter(filter)
	result, err := eslib.Client.Search(geoIndex).Query(query).Do(context.Background())
	printGeoResult(result, err)
}

//地理距离
func TestGeoDistance(t *testing.T) {
	query := elastic.NewBoolQuery()
	match := elastic.NewMatchAllQuery()
	filter := elastic.NewGeoDistanceQuery("location").GeoHash("31.191570,121.523288").Distance("200m")
	query.Must(match).Filter(filter)
	result, err := eslib.Client.Search(geoIndex).Query(query).Do(context.Background())
	printGeoResult(result, err)
}

//地理距离排序
func TestGeoDistanceSort(t *testing.T) {
	sort := elastic.NewGeoDistanceSort("location").
		Point(31.191570, 121.523288).
		Unit("km").
		SortMode("min").
		GeoDistance("arc").
		Asc()
	result, err := eslib.Client.Search(geoIndex).SortBy(sort).Do(context.Background())
	printGeoResult(result, err)
}

//多边形
func TestGeoPolyGon(t *testing.T) {
	query := elastic.NewBoolQuery()
	match := elastic.NewMatchAllQuery()
	filter := elastic.NewGeoPolygonQuery("location").
		AddPoint(31.192708, 121.522784).AddPoint(31.190395, 121.520531).
		AddPoint(31.189624, 121.52508).AddPoint(31.191882, 121.526067)
	query.Must(match).Filter(filter)
	result, err := eslib.Client.Search(geoIndex).Query(query).Do(context.Background())
	printGeoResult(result, err)
}

func printGeoResult(result *elastic.SearchResult, err error) {
	if err != nil {
		panic(err)
	}
	fmt.Println("total: ", result.TotalHits())
	each := result.Each(reflect.TypeOf(Student{}))
	fmt.Println(each)
}
