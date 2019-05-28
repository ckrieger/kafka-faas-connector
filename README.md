# Kafka FaaS Connector

The *Kafka FaaS Connector* is used by [MICO](https://github.com/UST-MICO/mico) to route messages from Kafka in the [CloudEvents](https://github.com/cloudevents/spec) format to an [OpenFaaS](https://github.com/openfaas/faas) function.

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

Build and push the Docker image:
```bash
docker build -t ustmico/kafka-faas-connector . && docker push ustmico/kafka-faas-connector
```

docker-compose:
```bash
docker-compose up --build kafka-faas-connector
```

## Misc

Actuator get configuration properties:
```bash
kubectl port-forward svc/kafka-faas-connector -n mico-workspace 8080
curl localhost:8080/actuator/configprops | jq . > configmaps.json
```

Kafka Producer (local):
```bash
./kafka-console-producer.sh --broker-list localhost:9092 --topic transform-request
```

Kafka Consumer (local):
```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transform-result --from-beginning
```

Sample message:
```json
{"specversion":"0.2","type":"io.github.ust.mico.result","source":"/router","id":"A234-1234-1234","time":"2019-05-08T17:31:00Z","contentType":"application/json","data":"test"}
```

curl the dummy function:
```bash
curl http://127.0.0.1:8080/function/dummy -d '{"specversion":"0.2","type":"io.github.ust.mico.result","source":"/router","id":"A234-1234-1234","time":"2019-05-08T17:31:00Z","contentType":"application/json","data":"test"}
'
```

Get logs:
```bash
kubectl -n $NAMESPACE logs -f $(kubectl get pods -n $NAMESPACE --selector=run=kafka-faas-connector --output=jsonpath={.items..metadata.name})
```

Delete Kuberentes pod:
```bash
kubectl -n $NAMESPACE delete pod $(kubectl get pods -n $NAMESPACE --selector=run=kafka-faas-connector --output=jsonpath={.items..metadata.name})
```

