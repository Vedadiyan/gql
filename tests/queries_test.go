package tests

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/vedadiyan/gql/pkg/functions/avg"
	_ "github.com/vedadiyan/gql/pkg/functions/count"
	_ "github.com/vedadiyan/gql/pkg/functions/mongo"
	_ "github.com/vedadiyan/gql/pkg/functions/nullifempty"
	_ "github.com/vedadiyan/gql/pkg/functions/unwind"
	"github.com/vedadiyan/gql/pkg/sql"
)

func TestHeavyZero(t *testing.T) {
	data, err := os.ReadFile("source.json")
	if err != nil {
		t.FailNow()
	}
	query, err := os.ReadFile("test.sql")
	if err != nil {
		t.FailNow()
	}
	topLevel := make(map[string]any)
	err = json.Unmarshal([]byte(data), &topLevel)
	if err != nil {
		t.FailNow()
	}
	then := time.Now()
	sql := sql.New(topLevel)
	sql.Prepare(string(query))
	now := time.Now()
	fmt.Println("prepared", now.Sub(then).Milliseconds())
	then = time.Now()
	rs, err := sql.Exec()
	fmt.Println(len(rs.([]any)))
	if err != nil {
		t.FailNow()
	}
	now = time.Now()
	fmt.Println(now.Sub(then).Milliseconds())
	json, _ := json.MarshalIndent(rs, "", "\t")
	os.WriteFile("output.json", json, os.ModePerm)
}
