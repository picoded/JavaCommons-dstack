# HazelcastConnector - config

Hazelcast instances can be deployed in either client or server mode.

Both modes will share the following common set of settings

| keyname           | type      | default    | description                                      |
|-------------------|-----------|------------|--------------------------------------------------|
| groupName         | string    |            | **Required** groupname of the cluster to join    |
| mode              | string    | server     | Instance mode to use for connection              |

Server mode will use the following additional config settings

| keyname           | type      | default    | description                                      |
|-------------------|-----------|------------|--------------------------------------------------|
| multicast         | boolean   | true       | Cluster scanning using multicast support         |
| port              | int       | 5900       | Server instance default port                     |
| portAutoIncrement | boolean   | true       | Auto increment port, if configured port is used  |
| memberTcpList     | array     | []         | Array of servers to scan for cluster detection   |

Client mode will use the following additional config settings

@TODO : Spec out and implement client mode

| keyname           | type      | default    | description                                      |
|-------------------|-----------|------------|--------------------------------------------------|
|                   |           |            |                                                  |

