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
		"$": {
			"numbers": [{
					"value": 1,
					"ok": true
				},
				{
					"value": 2,
					"ok": true
				},
				{
					"value": 3,
					"ok": true
				},
				{
					"value": 4,
					"ok": true
				},
				{
					"value": 500,
					"ok": true
				},
				{
					"value": 600,
					"ok": true
				},
				{
					"value": 700,
					"ok": true
				},
				{
					"value": 800,
					"ok": true
				},
				{
					"value": 9,
					"ok": true
				},
				{
					"value": 10,
					"ok": true
				}
			],
			"factors": [{
					"value": 1,
					"ok": false
				},
				{
					"value": 2,
					"ok": true
				},
				{
					"value": 3,
					"ok": false
				},
				{
					"value": 4,
					"ok": true
				},
				{
					"value": 5,
					"ok": true
				},
				{
					"value": 6,
					"ok": true
				},
				{
					"value": 7,
					"ok": true
				},
				{
					"value": 8,
					"ok": true
				},
				{
					"value": 9,
					"ok": true
				},
				{
					"value": 10,
					"ok": true
				}
			]
		}
	}
	`
	val := make(map[string]any)
	json.Unmarshal([]byte(test), &val)
	then := time.Now()
	sql := sql.New(val)
	sql.Prepare("SELECT * FROM `$.numbers` as A RIGHT JOIN `$.factors` as B on 1 = 1")
	rs, err := sql.Exec()
	if err != nil {
		t.FailNow()
	}
	now := time.Now()
	fmt.Println(now.Sub(then).Milliseconds())
	json, _ := json.MarshalIndent(rs, "", "\t")
	os.WriteFile("output.json", json, os.ModePerm)
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
	sql.Prepare("SELECT `Q1.rates.{0}` as first, `Q2.id` as second FROM `$.data.hotels` AS Q1 JOIN `$.data.hotels` AS Q2 ON `Q1.id` = `Q2.id` WHERE `Q1.id` = 'the_strand_palace'")
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
func TestHeavyProtobuf(t *testing.T) {
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
	sql.Prepare("select id, (select `match_hash`,`daily_prices` as rates, `meal`, (select (select `show_amount`, `currency_code`) as Amount from `payment_options.payment_types`) as payment_types from `rates` limit 1) as rates from `$.data.hotels` limit 1")
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
