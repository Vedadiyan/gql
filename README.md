
# GQL

GQL is an implementation of `MySQL` querying syntax for JSON. It is simply SQL for JSON.

  

GQL allows you to query up multi-dimensional data in complex and large JSON files at very high performance. The motivation behind writing this library was to use it together with `Protobuf` in order to bring about automatic mapping between `Message` structures and JSON data at runtime.

  

# What's Supported

 - [X] Subqueries
 - [X] Select Only Expression
 - [X] Case When
 - [X] Aliases 
 - [X] Like Expressions 
 - [X] Aggregated Functions (functions are extensible and can be injected when required)
 - [X] Singleton Functions (`ONCE` function only executes the function once for all rows) 
 - [X] Multi-Dimensional Selectors (`$.root.data.users.{?}.coordinates.{?}.{?}`)
 - [X] Limit
 - [ ] Joins (planned) *GQL does not require joins but this feature will be added for convenience in future versions* 
 - [ ] Group By (in development) 
 - [ ] Having Expression 

# Examples

    func Query(json map[string]any) {
        ctx := sql.New(json)
        result, err := ctx.Exec("SELECT ONCE(AVG(UNWIND(`$.root.data.users.{?}.age`))) as avg_of_age, name, `email.{0}` first_email FROM `$.root.data.users` WHERE `is_verified` = true")
    }

# IMPORTANT NOTES

Although this project is well tested, it is still in development.