# Picoded.JavaCommons.dstack

NoSQL abstraction layer between the application interacting with data, and the actual backend being used.

It facilitates the concept of layering of the application data storage, across multiple providers. Such as the following ...

* My / Oracle / MS SQL
* Distributed cache
* Object storage (Google Cloud Storage / AWS)

All while maintaining the same interface for the application. Leaving the decision of the backend used, or the combination of backend's used a devops responsibility.

Its most notable claass is `DataObjectMap` which functions as a `SQL` queryable `NoSQL` interface. That functions even against an SQL backend.
Followed by `DStack` which faciltates the stacking of data backend provider for devops. Such as a distributed cache with an SQL backend.

# Provider Support Matrix

| backend          | status        | notes                                                | DataObjectMap | KeyValueMap | KeyLongMap | FileWorkspaceMap |
|------------------|---------------|------------------------------------------------------|---------------|-------------|------------|------------------|
| struct.simple    | in-production | reference implementation, not recommended for use    | storage       | storage     | storage    | storage          |
| struct.cache     | in-production | local instance caching, useful for WORM data         | storage       | storage     |            |                  |
| jsql             | in-production | *with limits: see SQL support notes below            | full          | full        | full       | full             |
| mongodb          | in-production | *with limits: see MongoDB support notes below        | full          | full        | full       | full             |
| hazelcast.cache  | in-production |                                                      | full          | full        | full       |                  |
| hazelcast.store  | in-production |                                                      | full          | full        | full       |                  |
| file.simple      | in-production |                                                      |               |             |            | storage          |
| file.layered     | in-production |                                                      |               |             |            | storage          |
| resdisson        | experimental  |                                                      | storage       | storage     |            |                  |
| ignite           | roadmap       | roadmap                                              |               |             |            |                  |
| cockroachdb      | roadmap       | roadmap                                              |               |             |            |                  |

**Important notes**

- "full", means it has been optimized for both storage, and query operations.
- "storage" means does not support optimization for queries, for large queries, this can have detrimental performance implication, as the apistack will need to iterate a large number of data.
- Hazelcast require a custom build / deployment with the JavaCommons JAR file to support the required functionality
- MySQL connection / db seems to support only up to 16 digits of accuracy
- Requires read-after-write consistency, for expected behaviour, use w=majority&readConcernLevel=linearizable

# Data Structures

| DataStructure    | Status        | Description                                                                |
|------------------|---------------|----------------------------------------------------------------------------|
| DataObjectMap    | in-production | Map document storage, with SQL query support                               |
| KeyValueMap      | in-production | High performance key to string value storage                               |
| KeyLongMap       | in-production | Varient of KeyValue with atomic long support (if used in single tier mode) |
| FileWorkspaceMap | in-production | File workspace storage support                                             |
| MessageQueue     | road-map      | Message queue                                                              |
| JobQueue         | road-map      | Job request, response queue                                                |
