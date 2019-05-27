# Message Function Router

The *Message Function Router* is used by [MICO](https://github.com/UST-MICO/mico) to route message in the [CloudEvents](https://github.com/cloudevents/spec) format to the relevant functions ([OpenFaaS](https://github.com/openfaas/faas)).

## Requirements

Launch Kafka and ZooKeeper or establish a connection to already running instances (e.g. via port forwarding).

docker-compose:
```bash
docker-compose up kafka zookeeper
```

Port forwarding:
```bash
kubectl port-forward svc/kafka -n openfaas 9092
kubectl port-forward svc/zookeeper -n openfaas 2181
```

## Usage

Build the Docker image:
```bash
docker build -t ustmico/msg-function-router:latest
```

docker-compose:
```bash
docker-compose up --build msg-function-router
```

## Testing

Kafka Producer:
```bash
./kafka-console-producer.sh --broker-list localhost:9092 --topic transform-request
```

Kafka Consumer:
```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transform-result --from-beginning
```

Sample message:
```json
{"specversion":"0.2","type":"io.github.ust.mico.result","source":"/router","id":"A234-1234-1234","time":"2019-05-08T17:31:00Z","contentType":"application/json","data":"test"}
```

curl the dummy function:
``bash
curl http://127.0.0.1:8080/function/dummy -d '{"specversion":"0.2","type":"io.github.ust.mico.result","source":"/router","id":"A234-1234-1234","time":"2019-05-08T17:31:00Z","contentType":"application/json","data":"test"}
'
```
