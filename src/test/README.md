# Docker Commands to setup "testing DB" locally

## MongoDB

DB Setup

```
sudo docker run --name dstack-mongodb -p 27017:27017 -d mongo:5
```

Run Tests

```
./gradlew test -Ptest_all --tests picoded.dstack.mongodb.*
```

Cleanup DB

```
sudo docker stop dstack-mongodb;
sudo docker rm dstack-mongodb;
```