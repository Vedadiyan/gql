
# GQL
![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.19-%23007d9c)
[![Go Report Card](https://goreportcard.com/badge/github.com/vedadiyan/gql)](https://goreportcard.com/report/github.com/vedadiyan/gql)

GQL is an implementation of `MySQL` querying syntax for JSON. It is simply SQL for JSON.

  

GQL allows you to query up multi-dimensional data in complex and large JSON files at very high performance. The motivation behind writing this library was to use it together with `Protobuf` in order to bring about automatic mapping between `Message` structures and JSON data at runtime.

# Usage 
You can use GQL to re-model JSON data structures so that they can be auto mapped to your desired data models. For instance, if you are writing a microservice that retrieves data from a third-party API, you can focus on modeling internal data structures while using GQL to re-shape the output of that API to match the internal data model. Once this is done, the output of GQL can be automatically mapped to the internal data model.  

# What's Supported

 - [X] Subqueries
 - [X] Select Only Expressions
 - [X] Case When
 - [X] Aliases 
 - [X] Like Expressions 
 - [X] Aggregate Functions (GQL functions are extensible and can be injected when required)
 - [X] Singleton Functions (The `ONCE` function only executes the function once for all rows) 
 - [X] Multi-Dimensional Selectors (`$.root.data.users.{?}.coordinates.{?}.{?}`)
 - [X] Limit
 - [X] Group By
 - [X] Joins
   - [X] INNER JOIN
   - [X] LEFT JOIN
   - [X] RIGHT JOIN
   - [ ] NATURAL JOIN *There is no plan to implement this feature*
   - [ ] NATURAL LEFT JOIN *There is no plan to implement this feature*
   - [ ] NATURAL RIGHT JOIN *There is no plan to implement this feature*
 - [X] CTEs 
 - [ ] Having Expression (in development) 

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

*Please note that although multi-dimensional selectors such as `{?}.{?}` are supported, the `FROM` clause does not support multi-dimensional selectors. However, the following is valid `$.data.items.{0}.rates` *

# Using Functions 
To use functions, simply import them from the `function` package:

    import (
	    _ "github.com/vedadiyan/gql/pkg/functions/avg"
	    _ "github.com/vedadiyan/gql/pkg/functions/once"
	    _ "github.com/vedadiyan/gql/pkg/functions/unwind"
    )

# IMPORTANT NOTES

Although this project is well tested, it is still in development.
