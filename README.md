#**IMPORTANT NOTICE**
GQL is now fully re-written and enhanced. The repository has been moved to this link https://github.com/Vedadiyan/genql however this repository will remain as it is since many of our project already use this repository and have referenced it. 

# GQL (General Querying Language)
GQL is an implementation of `MySQL` dialect for querying data in complex data structures.



GQL allows you to query data in large and complex data structures at very high performance. The motivation behind writing this library was to use it together with `Protobuf` in order to bring about automatic mapping between `Message` structures and JSON data at runtime.

# SQL Interpretation
GQL relies on a modified version of the `sqlparser` package in the Vitess project. It is guaranteed to parse SQL code flawlessly. 

# Usage 
You can use GQL to re-model JSON data structures so that they can be auto mapped to your desired data models. For instance, if you are writing a microservice that retrieves data from a third-party API, you can focus on modeling internal data structures while using GQL to re-shape the output of that API to match the internal data model. Once this is done, the output of GQL can be automatically mapped to the internal data model.  

# 📌 What's Supported

 - ✅ Subqueries
 - ✅ Select Only Expressions
 - ❌ Multiple Object Selection *(Statements such as `SELECT * FROM object_01, object_02` are not supported)*
 - ✅ Case When
 - ✅ Aliases
 - ✅ Like Expressions
 - ✅ Aggregate Functions (GQL functions are extensible and can be injected when required)
 - 🆒 Singleton Functions (The `ONCE` function only executes the function once for all rows)
 - 🆒 Multi-Dimensional Selectors (`$.root.data.users.{?}.coordinates.{?}.{?}`)
 - ✅ Limit
 - ✅ Group By
 - ❎ Joins (Joins are experimental and may require performance tuning)
	 - ✅ INNER JOIN
	 - ✅ LEFT JOIN
	 - ✅ RIGHT JOIN
	 - ⭕ FULL OUTER JOIN *(MySQL does not have full outer joins and GQL is restricted by the MySQL syntax)*
	 - ❌ NATURAL JOIN *(There is no plan to implement this feature)*
	 - ❌ NATURAL LEFT JOIN *(There is no plan to implement this feature)*
	 - ❌ NATURAL RIGHT JOIN *(There is no plan to implement this feature)*
 - ⭕ Apply / Cross Apply *(MySQL does not have apply / cross apply and GQL is restricted by the MySQL syntax)*
 - ✅ Unions
 - ✅ CTEs
 - ✅ Having Expression (experimental)
 - ✅ Order By

# Caveats
- Join conditions ALWAYS require table aliasing even if they are used in a CTE query 
- Keys should always be specified within backticks. For example: `` `a.b` `` is valid while `a.b` is not.
- An array selector keeps the data type and always returns an array even though a single index is selected 

# Examples

1- Basic Example

    func Query(json map[string]any) {
        ctx := sql.New(json)
        result, err := ctx.Exec("SELECT ONCE(AVG(UNWIND(`$.root.data.users.{?}.age`))) as avg_of_age, name, `email.{0}` first_email FROM `$.root.data.users` WHERE `is_verified` = true")
    }

2- Encapsulation

    SELECT (SELECT `price`, `quantity`) AS stock  FROM `$.data.items`    

This query will retrieve `price` and `quantity` from the row and will turn them into a new object called `stock` which will be created per row.

3- Arrays

    SELECT (SELECT `amount` FROM `tax_data`) AS taxes  FROM `$.data.items` 

This query will retrieve `amount` from an array of objects called `tax_data`. 

4- Array Selectors 

    SELECT `rates.{0}` AS first_item FROM `$.data.items` WHERE `rates.{?}.amount` > 10

Array indexes can be reached using the `{}` selector. You can pass either a number or a wildcard using the `{?}` to select and query arrays.

~~*Please note that although multi-dimensional selectors such as `{?}.{?}` are supported, the `FROM` clause does not support multi-dimensional selectors. However, the following is valid `$.data.items.{0}.rates`*~~

*Updated in the v0.0.3, multi-dimensional selectors can be used in the `FROM` clause*

5- Look-Behind and Root Selectors

	SELECT 
		(
			SELECT
				`$.$.another_object.name` AS `name`
			FROM `details` 
		)
	FROM 
		`$.data`

The `FROM` block scopes the reading to the current row. In order to read data not associated with the current row, the Look-Behind or Root Selector feature can be used to navigate back to the previous object. This can be done by using the `$` operator. 

# Using Functions 
To use functions, simply import them from the `function` package:

    import (
	    _ "github.com/vedadiyan/gql/pkg/functions/avg"
	    _ "github.com/vedadiyan/gql/pkg/functions/once"
	    _ "github.com/vedadiyan/gql/pkg/functions/unwind"
    )

# IMPORTANT NOTES

Although this project is well tested, it is still in development.

## 📝 License

Copyright © 2023 [Pouya Vedadiyan](https://github.com/vedadiyan).

This project is [Apache-2.0](./LICENSE) licensed.

