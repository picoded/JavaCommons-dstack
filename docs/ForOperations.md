# DStack for Operations

DStack facilitate the configuration of data layers for operations. Allowing operations to optimize each individual data layer, in accordence for its usage pattern.

For example:

+ Authentication KeyValueMap could be stored in high performance in-ram storage such as redis.
+ DataObject query data can be configured with a combination of hazelcast (for quick get caching) and mysql (for query)
+ FileObject storage can be stored in hot glusterfs, followed by cold GCP storage

So how is this done?

## DStack.json config

Lets start with defining the settings for the backend inside dstack.json,
these backends will be used in various combination for the datastructures requried.

~~~{.json}
{ 
	...

	//
	// Define the backends used in the application
	// and its default settings, for used with a datastructure
	//
	// Note that this is evaluated in the priority order
	// that it is stated in this backend
	// 
	"backend" : [
		//
		// Limited capcity low latency cache
		// Used mainly for temporary credential session
		//
		{
			// Name of the backend, must be unique
			// (name uses the type parameter by default)
			"name" : "jcache-redis",
			
			// Configure the redis backend settings
			"type" : "jcache-redis",

			// Redis settings
			"host" : "localhost",
		},

		//
		// Good old reliable
		// SQL Layer used by the backend
		// 
		{
			// Nane of the backend, must be unique
			// (name uses the type parameter by default)
			"name" : "jsql-mysql",
			
			// Configuring the mysql backend settings
			"type" : "jsql-mysql",
			
			// Mysql settings
			"user"   : "sql-user",
			"pass"   : "sql-pass",
			"dbname" : "database-name",
			"host"   : "localhost",
			"port"   : "3306",
		},

		//
		// Persistent file storage
		//
		{
			// Nane of the backend, must be unique
			// (name uses the type parameter by default)
			"name" : "file-glusterfs",
			
			// Configuring the glusterfs backend settings
			"type" : "file-glusterfs",
			
			// Glusterfs settings
			"volume"  : "hot-storage",
			"subpath" : "",
			"host"    : "localhost",
		}
	],

	...
}
~~~

Once the backend is configured, next will be to configure the 
provider settings, for specific data structures.

~~~
{
	...

	//
	// provider rullings to use.
	// this is evaluated in a top to down manner, 
	// terminating on the first match.
	//
	"provide": [
		//
		// FileWorkspacema specific overwrite
		//
		{
			// Namespace based matching
			"match" : ["project", "test"],

			// Indicate support only for FileWorkspaceMap
			"FileWorkspaceMap" : true,

			// Backend to use
			"backend" : "file-glusterfs",
		}

		//
		// KeyLongMap specific overwrite,
		// because there can only be one KeyLongMap backend
		// at a time to properly support atomic operations.
		//
		{
			// Regex based matching of namespace to provide
			"regex" : ".*",

			// Indicate support for KeyLongMap specifically
			"KeyLongMap" : true,

			// Backend to use (by name)
			"backend" : "jcache-redis"
		},

		//
		// Final fallback provider setting for all datastructure types
		//
		{
			// Regex based matching of namespace to provide
			"regex" : ".*",

			// Indicate if its used by default
			// for any dstack datastructure
			"any" : true,

			// The backends used, identified by id
			"backend" : [ {
				// Name of backend to use 
				// declared in "backend" list
				"name" : "jcache-redis",

				// This disable query using jcache
				// for dataobject which is "slow" anyway
				"query" : false
			}, {
				// Name of backend to use 
				// declared in "backend" list
				"name" : "jsql-mysql",

				// This enable query using jsql
				"query" : "all"
			} ]
		}
	],

	...
}
~~~