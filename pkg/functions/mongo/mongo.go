package mongo

import (
	"bytes"
	"context"
	"encoding/json"
	"text/template"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Mongo(jo *[]any, row any, args []any) any {
	result, err := readArgsGeneric(args, row, jo)
	if err != nil {
		return err
	}
	var buff bytes.Buffer
	err = result.(map[string]any)["query"].(*template.Template).Execute(&buff, result.(map[string]any)["params"])
	if err != nil {
		return err
	}
	mapper := result.(map[string]any)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(mapper["connection"].(string)))
	if err != nil {
		return err
	}
	var filter bson.A
	databse := client.Database(mapper["database"].(string))
	collection := databse.Collection(mapper["collection"].(string))
	err = json.Unmarshal(buff.Bytes(), &filter)
	if err != nil {
		return err
	}
	res, err := collection.Aggregate(context.TODO(), filter)
	if err != nil {
		return err
	}
	data := make([]any, 0)
	for res.Next(context.TODO()) {
		dataMap := make(map[string]any)
		json.Unmarshal([]byte(res.Current.String()), &dataMap)
		data = append(data, dataMap)
	}
	return data
}
func readArgsGeneric(args []any, row any, jo *[]any) (any, error) {
	mapper := make(map[string]any)
	connection := func(args any) error {
		mapper["connection"] = args.(string)
		return nil
	}
	database := func(args any) error {
		mapper["database"] = args.(string)
		return nil
	}
	collection := func(args any) error {
		mapper["collection"] = args.(string)
		return nil
	}
	params := func(args any) error {
		mapper["params"] = args.([]any)[0]
		return nil
	}
	query := func(args any) error {
		res, err := template.New("").Parse(args.(string))
		if err != nil {
			return err
		}
		mapper["query"] = res
		return nil
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.STRING, functions.STRING, functions.STRING, functions.ANY, functions.STRING}, []functions.Reader{connection, database, collection, params, query})
	if err != nil {
		return nil, err
	}
	return mapper, nil
}

func init() {
	cmn.RegisterFunction("mongo", Mongo)
}
