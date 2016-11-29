12-factor microservices
=======================

### Initial setup

```
# start a postgres container
docker run --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=password -e POSTGRES_DB=babynames -d postgres

# start a kafka container
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=127.0.0.1 --env ADVERTISED_PORT=9092 -d spotify/kafka

# import the raw data
go run baby-names-import/main.go
```

### 12-factor microservices

```
# start the baby names API
go run 12-factor-microservices/baby-names-api/main.go

# query the baby names API
curl localhost:8080/top10 | jq

# start the baby names input API
go run 12-factor-microservices/baby-names-input-api/main.go

# register a new baby
curl -v -d '{"name": "OLIVER", "sex": "male"}' localhost:8081/baby

# query the baby names API
curl localhost:8080/top10 | jq
```

### Event driven architecture

```
# start the baby names API
go run event-driven-architecture/baby-names-api/main.go

# query the baby names API
curl localhost:8080/top10 | jq

# start the baby names input API
go run event-driven-architecture/baby-names-input-api/main.go

# register a new baby
curl -v -d '{"name": "OLIVER", "sex": "male"}' localhost:8081/baby

# start the baby names processor
go run event-driven-architecture/baby-names-processor/main.go

# start the baby names streaming API
go run event-driven-architecture/baby-names-streaming-api/main.go

# follow the stream
curl -v localhost:8082/stream

# register a new baby
curl -v -d '{"name": "OLIVER", "sex": "male"}' localhost:8081/baby
```
