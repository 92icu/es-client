package signle

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	eslib "gitlab.blackshark.com/golibs/esclient"
	"reflect"
	"testing"
)

type GeoData struct {
	Username string `json:"username"`
	Location string `json:"location"`
}

const geoIndex = "geo_test"

func init() {
	eslib.Init("http://10.0.12.211:9200,http://10.0.12.212:9200,http://10.0.12.222:9200", "elastic", "elastic")
}

func TestAddData(t *testing.T) {
	mapping := `{
    "properties": {
      "location": {
        "type": "geo_point"
      }
    }
}`
	if ok, err := eslib.PutMapping(geoIndex, mapping); err != nil {
		panic(err)
	} else {
		fmt.Println(ok)
	}

	datas := []interface{}{
		&GeoData{Username: "Jame", Location: "31.191570,121.523288"},
		&GeoData{Username: "Jame1", Location: "31.191643,121.527826"},
		&GeoData{Username: "Jame2", Location: "31.190303,121.523835"},
		&GeoData{Username: "Jame3", Location: "31.203575,121.557634"},
		&GeoData{Username: "Jame4", Location: "31.142267,121.808682"},
	}
	eslib.Bulk(geoIndex, datas)
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
	each := result.Each(reflect.TypeOf(GeoData{}))
	fmt.Println(each)
}
