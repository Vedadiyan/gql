package tests

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/vedadiyan/gql/pkg/functions/avg"
	_ "github.com/vedadiyan/gql/pkg/functions/once"
	_ "github.com/vedadiyan/gql/pkg/functions/unwind"

	"github.com/vedadiyan/gql/pkg/sql"
)

func TestSQL(t *testing.T) {
	test := `
	{
		"numbers": [
			{
				"id": 1,
				"name": "Pouya",
				"email": "vedadiyan@gmail.com",
				"test": [
					{
						"ok": "ok",
						"then": [
							1
						]
					}
				]
			},
			{
				"id": 2,
				"name": "Vedadiyan"
			}
		]
	}
	`
	val := make(map[string]any)
	json.Unmarshal([]byte(test), &val)
	then := time.Now()
	sql := sql.New(val)
	sql.Prepare("SELECT hello FROM `numbers.{?}.test.{?}` WHERE ok = 'ok'")
	sql.Exec()
	now := time.Now()
	fmt.Println(now.Sub(then).Microseconds())
}

func TestHeavyZero(t *testing.T) {
	data, err := os.ReadFile("source.json")
	if err != nil {
		t.FailNow()
	}
	topLevel := make(map[string]any)
	err = json.Unmarshal([]byte(fmt.Sprintf(`{"$": %s}`, data)), &topLevel)
	if err != nil {
		t.FailNow()
	}
	then := time.Now()
	sql := sql.New(topLevel)
	sql.Prepare("SELECT ONCE(AVG(UNWIND(UNWIND(`$.rates.{?}.daily_prices`)))) as Price FROM `$.data.hotels` LIMIT 2 OFFSET 10 --WHERE `rates.{?}.payment_options.payment_types.{?}.show_amount` = '2003.00' --not like '%als' and `ref` = CASE WHEN `test` BETWEEN 0 AND 2 THEN 'small' WHEN `test` BETWEEN 100 AND 500 THEN 'medium' ELSE 'large' END")
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
func TestHeavy(t *testing.T) {
	data, err := os.ReadFile("large-file.json")
	if err != nil {
		t.FailNow()
	}
	topLevel := make(map[string]any)
	err = json.Unmarshal([]byte(fmt.Sprintf(`{"$": %s}`, data)), &topLevel)
	if err != nil {
		t.FailNow()
	}
	then := time.Now()
	sql := sql.New(topLevel)
	sql.Prepare("SELECT ONCE(AVG(UNWIND(UNWIND(`$.Q.Test`)))) as `Index` FROM (SELECT `Hotels.rates.{?}.daily_prices` as Test FROM `$.data.hotels` as Hotels) Q --WHERE `Q.Test.{0}.{?}` = '250.37'")
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

func TestCaseWhenHeavy(t *testing.T) {
	data, err := os.ReadFile("large-file.json")
	if err != nil {
		t.FailNow()
	}
	topLevel := make(map[string]any)
	err = json.Unmarshal([]byte(fmt.Sprintf(`{"$": %s}`, data)), &topLevel)
	if err != nil {
		t.FailNow()
	}
	then := time.Now()
	sql := sql.New(topLevel)
	sql.Prepare("SELECT `id`, `rates.{?}.daily_prices` as daily_prices FROM `$.data.hotels` WHERE `ref` = CASE WHEN `test` BETWEEN 0 AND 2 THEN 'small' WHEN `test` BETWEEN 100 AND 500 THEN 'medium' ELSE 'large' END")
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
}

func TestHeavyFrom(t *testing.T) {
	data, err := os.ReadFile("large-file.json")
	if err != nil {
		t.FailNow()
	}
	topLevel := make(map[string]any)
	err = json.Unmarshal([]byte(fmt.Sprintf(`{"$": %s}`, data)), &topLevel)
	if err != nil {
		t.FailNow()
	}
	then := time.Now()
	flatten, err := sql.From(topLevel, "$.abc")
	if err != nil {
		t.FailNow()
	}
	_ = flatten
	now := time.Now()
	fmt.Println("flattened", now.Sub(then).Milliseconds())
}
