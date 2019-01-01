## JSql - config

JSql instances can be deployed against any of the following backend

+ SQLite
+ MySQL
+ MSSQL
+ Oracle (@TODO)
+ Postgres (@TODO)

The above backend is selected using the type parameter. With the selected backend above in lowercase

| keyname           | type      | default    | description                                      |
|-------------------|-----------|------------|--------------------------------------------------|
| type              | string    |            | **Required** backend engine to connect to        |

For SQLite an additional path parameter is required for sqlite file path. In memory mode is supported

| keyname           | type      | default    | description                                      |
|-------------------|-----------|------------|--------------------------------------------------|
| path              | string    | :memory:   | **Required** sqlite file path, can be ':memory:' |

For all other server based SQL mode, tcp connection is supported with the following

| keyname           | type      | default      | description                                      |
|-------------------|-----------|--------------|--------------------------------------------------|
| host              | string    | localhost    | hostname or ip address to connect to             |
| port              | int       | type-default | port to connect to, default depends on type      |
| name              | string    |              | **Required** database name to connect to         |
| user              | string    |              | **Required** username to use                     |
| pass              | string    |              | **Required** password to use                     |

