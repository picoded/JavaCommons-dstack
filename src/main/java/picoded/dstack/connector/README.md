# dstack.connector

Contains multiple XConnector classes, which provides a connection to the respective backend, given the required config map.

Examples include, but not limited to

+ HazelcastConnector
+ JSqlConnector

Every connection object returned by the XConnector class is **required** to be thread safe, and handles any connection pooling internally if needed. 
Returned connector object is expected to be use concurrently by multiple threads.

Connection object maybe vendor / backend specific.

XConnector classes, are requried to have the following public static methods. Which would return the backend specific connector object, refered below as V

+ `V getConnection(GenericConvertMap<String,Object> config)`
+ `void closeConnection(V connection)`

If null argument was provided, the following MUST be performed `throw new IllegalArgumentException(ExceptionMessage.unexpectedNullArgument);`

If the object V class/interface is not an external library implmentation, but a JavaCommons implmentation / wrapper (such as JSQL).
Its returning class/interface V, must be in the `dstack.connector` namespace. Textual representation of V should (but not required to) be identical to X.

XConnector classes may implement internal classes utility files, under its own namespace. For example JSql, would implement utility classes under `dstack.connector.jsql.*`

## HazelcastConnector - config