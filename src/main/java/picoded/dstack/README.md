# picoded.dstack

DStack is an abstraction layer, designed to facilitate usage of various data layers,
from SQL, to no-SQL. And provide a single consistent interface for the developer. 

Initializing it, starts with a map/json configuration such as the following ...

``` js
{
	//
	// sys.DStack.provider
	//
	// This represent the various data layers providers used in the system.
	// Normally a caching layer will be placed first, followed by
	// the actual implmentation layer.
	//
	// The last layer will normally be the "single source of truth",
	// for handling data accuracy resolution. (Normally JSql)
	//
	"provider" : [
		//--------------------------------------------------------------------------------
		//
		// Server instance caching
		//
		//--------------------------------------------------------------------------------
		{
			"type" : "StructCache",
			"name" : "instance_cache"
		},
		//--------------------------------------------------------------------------------
		//
		// Layer 0 : The final layer of the DStack,
		//
		// This would be the "single source of truth"
		// for subsequent data layers above it
		//
		//--------------------------------------------------------------------------------
		{
			// Data stack type
			"type" : "jsql",
			"name" : "main_db",
			"db" : {
				// [MY-SQL] name implmentation
				"type" : "mysql",
				"path" : "some_mysql_db:3306",
				"name" : "database_name",
				"user" : "database_user",
				"pass" : "database_pass",
			}
		}
	],

	//
	// sys.DStack.namespace
	//
	// Declaration of namespace relation to the providers
	//
	"namespace" : [
		{
			"regex" : ".*",
			"providers" : [
				"instance_cache",
				"main_db"
			]
		}
	]
}
```

With the above configuration, the DStack object can then be subsequently initialized.

``` java
dstackObj = new DStack(dstackConfig());
```

Diffirent data structure can then be subsequently loaded from it

``` java
// DataObjectMap - used as the primary "database table"
dstackObj.dataObjectMap("table_name");

// KeyValueMap - primarily used for high performance key value storage, with expiration timestamp
//               useful timestamp, and performance sensitive workload (such as auth sessions)
dstackObj.keyValueMap("table_name");

// KeyLongMap - varient of KeyValueMap focused primarily on numerical values, and atomic operations
//              note that for atomic operations would require its namespace to be initialized with a single provider
dstackObj.keylongMap("table_name");

// FileWorkspaceMap - Object file storage backend, used to easily swap out storage backends from S3 to FileServers
dstackObj.FileWorkspaceMap("table_name");
```