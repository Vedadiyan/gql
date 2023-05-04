package tests

import (
	"encoding/json"
	"testing"

	"github.com/vedadiyan/gql/pkg/sql"
)

func TestSimpleKeySelector(t *testing.T) {
	input := `
{
	"data": [
		{
			"target": {
				"value": 0
			}
		}
	]
}
	`
	expectation := []any{
		map[string]any{
			"result": 0,
		},
	}
	base(&input, expectation, "SELECT `target.value` AS result FROM `$.data`", t)
}

func TestMultipleKeySelector(t *testing.T) {
	input := `
{
	"data": [
		{
			"target": {
				"target_2": {
					"target_3": {
						"value": 0
					}
				}
			}
		}
	]
}
	`
	expectation := []any{
		map[string]any{
			"result": 0,
		},
	}
	base(&input, expectation, "SELECT `target.target_2.target_3.value` AS result FROM `$.data`", t)
}

func TestInvalidKeySelector(t *testing.T) {
	input := `
{
	"data": [
		{
			"target": {
				"target_2": {
					"target_3": {
						"value": 0
					}
				}
			}
		}
	]
}
	`
	expectation := []any{
		map[string]any{
			"result": nil,
		},
	}
	base(&input, expectation, "SELECT `target.target_INVALID.target_3.value` AS result FROM `$.data`", t)
}

func TestSimpleIndexedArraySelector(t *testing.T) {
	input := `
{
	"data": [	
		{
			"target": [
				{
					"value": 0
				}
			]
		}
	]
}
	`
	expectation := []any{
		map[string]any{
			"result": []any{0},
		},
	}
	base(&input, expectation, "SELECT `target.{0}.value` AS result FROM `$.data`", t)
}

func TestTwoDimensionaIndexedArraySelector(t *testing.T) {
	input := `
{
	"data": [	
		{
			"target": [
				[
					{
						"value": 0
					}
				]
			]
		}
	]
}
	`
	expectation := []any{
		map[string]any{
			"result": []any{0},
		},
	}
	base(&input, expectation, "SELECT `target.{0}.{0}.value` AS result FROM `$.data`", t)
}

func TestThreeDimensionaIndexedArraySelector(t *testing.T) {
	input := `
{
	"data": [	
		{
			"target": [
				[
					[
						{
							"value": 0
						}
					]
				]
			]
		}
	]
}
	`
	expectation := []any{
		map[string]any{
			"result": []any{0},
		},
	}
	base(&input, expectation, "SELECT `target.{0}.{0}.{0}.value` AS result FROM `$.data`", t)
}

func TestThreeDimensionaIndexedArrayWithWildCardSelectorVar1(t *testing.T) {
	input := `
{
	"data": [	
		{
			"target": [
				[
					[
						{
							"value": 0
						}
					]
				]
			]
		}
	]
}
	`
	expectation := []any{
		map[string]any{
			"result": []any{0},
		},
	}
	base(&input, expectation, "SELECT `target.{?}.{0}.{?}.value` AS result FROM `$.data`", t)
}

func TestThreeDimensionaIndexedArrayWithWildCardSelectorVar2(t *testing.T) {
	input := `
{
	"data": [	
		{
			"target": [
				[
					[
						{
							"value": 0
						}
					]
				]
			]
		}
	]
}
	`
	expectation := []any{
		map[string]any{
			"result": []any{0},
		},
	}
	base(&input, expectation, "SELECT `target.{0}.{?}.{?}.value` AS result FROM `$.data`", t)
}

func TestThreeDimensionaIndexedArrayWithWildCardSelectorVar3(t *testing.T) {
	input := `
{
	"data": [	
		{
			"target": [
				[
					[
						{
							"value": 0
						}
					]
				]
			]
		}
	]
}
	`
	expectation := []any{
		map[string]any{
			"result": []any{0},
		},
	}
	base(&input, expectation, "SELECT `target.{0}.{?}.{0}.value` AS result FROM `$.data`", t)
}

func TestLeadingIndexArraySelector(t *testing.T) {
	input := `
{
	"data": [	
		{
			"target": [
				[
					[
						{
							"value": 0
						},
						{
							"value": 2
						}
					]
				]
			]
		}
	]
}
	`
	expectation := []any{
		map[string]any{
			"result": []any{0, 2},
		},
	}
	base(&input, expectation, "SELECT `{?}.{?}.value` AS result FROM `$.data.{?}.target`", t)
}
func base(input *string, expectation any, query string, t *testing.T) {
	if input == nil {
		t.FailNow()
	}
	mapper := make(map[string]any)
	err := json.Unmarshal([]byte(*input), &mapper)
	if err != nil {
		t.FailNow()
	}
	sql := sql.New(mapper)
	err = sql.Prepare(query)
	if err != nil {
		t.FailNow()
	}
	result, err := sql.Exec()
	if err != nil {
		t.FailNow()
	}
	outputJson, err := json.Marshal(result)
	if err != nil {
		t.FailNow()
	}
	expectationJson, err := json.Marshal(expectation)
	if err != nil {
		t.FailNow()
	}
	if string(outputJson) != string(expectationJson) {
		t.FailNow()
	}
}
