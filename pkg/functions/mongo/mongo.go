package mongo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"sync"
	"text/template"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoArgs struct {
	Collection *mongo.Collection
	Query      bson.A
}

var (
	_conManager func(connKey string) (*mongo.Client, error)
	_templates  sync.Map
)

func Mongo(jo *[]any, row any, args []any) (any, error) {
	mongoArgs, err := readArgsGeneric(args, row, jo)
	if err != nil {
		return nil, err
	}
	res, err := mongoArgs.Collection.Aggregate(context.TODO(), mongoArgs.Query)
	if err != nil {
		return nil, err
	}
	data := make([]any, 0)
	for res.Next(context.TODO()) {
		dataMap := make(map[string]any)
		err := json.Unmarshal([]byte(res.Current.String()), &dataMap)
		if err != nil {
			return nil, err
		}
		data = append(data, dataMap)
	}
	return data, nil
}
func readArgsGeneric(args []any, row any, jo *[]any) (*MongoArgs, error) {
	mongoArgs := MongoArgs{}
	var conn *mongo.Client
	var dbname string
	var param any
	connection := func(args any) error {
		_conn, err := _conManager(args.(string))
		if err != nil {
			return err
		}
		conn = _conn
		return nil
	}
	database := func(args any) error {
		dbname = args.(string)
		return nil
	}
	collection := func(args any) error {
		mongoArgs.Collection = conn.Database(dbname).Collection(args.(string))
		return nil
	}
	params := func(args any) error {
		param = args
		return nil
	}
	query := func(args any) error {
		text := args.(string)
		hash, err := getHash([]byte(text))
		if err != nil {
			return nil
		}
		var query *template.Template
		oldQuery, ok := _templates.Load(hash)
		if ok {
			query = oldQuery.(*template.Template)
		} else {
			newQuery, err := template.New(hash).Parse(args.(string))
			if err != nil {
				return err
			}
			query = newQuery
			_templates.Store(hash, query)
		}
		var buff bytes.Buffer
		err = query.Execute(&buff, param)
		if err != nil {
			return err
		}
		err = json.Unmarshal(buff.Bytes(), &mongoArgs.Query)
		if err != nil {
			return err
		}
		return nil
	}
	err := functions.CheckSingnature(args, []functions.ArgTypes{functions.STRING, functions.STRING, functions.STRING, functions.ANY, functions.STRING}, []functions.Reader{connection, database, collection, params, query})
	if err != nil {
		return nil, err
	}
	return &mongoArgs, nil
}

func getHash(bytes []byte) (string, error) {
	sha := sha256.New()
	_, err := sha.Write(bytes)
	if err != nil {
		return "", err
	}
	hash := base64.StdEncoding.EncodeToString(sha.Sum(nil))
	return hash, nil
}

func RegisterConManager(fn func(connKey string) (*mongo.Client, error)) {
	_conManager = fn
}

func init() {
	cmn.RegisterFunction("mongo", Mongo)
}
