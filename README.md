# Picoded.JavaCommons.dstack

NoSQL abstraction layer between the application interacting with data, and the actual backend being used.

It facilitates the concept of layering of the application data storage, across multiple providers. Such as the following ...

* My / Oracle / MS SQL
* Distributed cache
* Object storage (Google Cloud Storage / AWS)

All while maintaining the same interface for the application. Leaving the decision of the backend used, or the combination of backend's used a devops responsibility.

Its most notable claass is `DataObjectMap` which functions as a `SQL` queryable `NoSQL` interface. That functions even against an SQL backend.
Followed by `DStack` which faciltates the stacking of data backend provider for devops. Such as a distributed cache with an SQL backend.

# Support Matrix

| backend       | status        | notes                                                | DataObjectMap | KeyValueMap | KeyLongMap | FileWorkspaceMap |
|---------------|---------------|------------------------------------------------------|---------------|-------------|------------|------------------|
| struct.simple | development   | reference implementation, not for used in production | storage       | storage     | storage    | storage          |
| struct.cache  | development   |                                                      | storage       |             |            |                  |
| jsql          | in-production | see SQL support table below for details              | full          | full        | full       | full             |
| hazelcast     | development   |                                                      | storage       | storage     |            |                  |
| file.simple   | development   |                                                      |               |             |            | development      |
| ignite        | development   | roadmap                                              |               |             |            |                  |
| cockroachdb   | development   | roadmap                                              |               |             |            |                  |

# Data Structures

| DataStructure    | Status        | Description                                                                |
|------------------|---------------|----------------------------------------------------------------------------|
| DataObjectMap    | in-production | Map document storage, with SQL query support                               |
| KeyValueMap      | in-production | High performance key to string value storage                               |
| KeyLongMap       | experimental  | Varient of KeyValue with atomic long support (if used in single tier mode) |
| FileWorkspaceMap | development   | File workspace storage support                                             |
| MessageQueue     | road-map      | Message queue                                                              |
| JobQueue         | road-map      | Job request, response queue                                                |
