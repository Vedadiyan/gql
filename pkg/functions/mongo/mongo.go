package mongo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Mongo(jo *[]any, row any, args []any) any {
	result, err := readArgs(args, row, jo)
	if err != nil {
		return err
	}
	mapper := result.(map[string]string)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://booqall:Pou96179617!@206.189.26.125:27017"))
	if err != nil {
		return err
	}
	var filter bson.M
	if mapper["collection"] == "Hotels" {
		err = json.Unmarshal([]byte(fmt.Sprintf(`{"code": {"$in": %s}}`, result.(map[string]string)["query"])), &filter)
	} else {
		err = json.Unmarshal([]byte(fmt.Sprintf(`{"name": {"$in": %s}}`, result.(map[string]string)["query"])), &filter)
	}
	then := time.Now()
	res, err := client.Database("RateHawk").Collection(mapper["collection"]).Find(context.TODO(), filter)
	data := make([]any, 0)
	for res.Next(context.TODO()) {
		dataMap := make(map[string]any)
		json.Unmarshal([]byte(res.Current.String()), &dataMap)
		data = append(data, dataMap)
	}
	now := time.Now()
	_ = err
	_ = res
	_ = mapper
	fmt.Println("mongo time", now.Sub(then).Seconds())
	return data
}
func readArgs(args []any, row any, jo *[]any) (any, error) {
	mapper := make(map[string]string)
	connection := func(args any) error {
		mapper["connection"] = args.(string)
		return nil
	}
	collection := func(args any) error {
		mapper["collection"] = args.(string)
		return nil
	}
	query := func(args any) error {
		buffer := bytes.NewBufferString("[")
		for _, v := range args.([]any) {
			buffer.WriteString("\"")
			buffer.WriteString(v.(map[string]any)["id"].(string))
			buffer.WriteString("\"")
			buffer.WriteString(",")
		}
		buffer.Truncate(buffer.Len() - 1)
		buffer.WriteString("]")
		mapper["query"] = buffer.String()
		return nil
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.STRING, functions.STRING, functions.ANY}, []functions.Reader{connection, collection, query})
	if err != nil {
		return nil, err
	}
	return mapper, nil
}

func init() {
	cmn.RegisterFunction("mongo", Mongo)
}
