# Kafka FaaS Connector

The *Kafka FaaS Connector* is used by [MICO](https://github.com/UST-MICO/mico) to route messages from Kafka in the [CloudEvents](https://github.com/cloudevents/spec) format to an [OpenFaaS](https://github.com/openfaas/faas) function.

## Requirements

*Kafka FaaS Connector* requires Kafka and OpenFaaS.

### Kubernetes

Run Kafka, ZooKeeper and OpenFaaS in your Kubernetes cluster.
You find some notes about Kafka in our [documentation](https://mico-docs.readthedocs.io/en/latest/setup/kubernetes/kafka.html).

Modify `kafka-faas-connector.yml` to set the correct URLs of the OpenFaaS gateway (default: `http://gateway.openfaas:8080`) and the Kafka servers (default: `bootstrap.kafka:9092`).

### Local execution

Launch Kafka and ZooKeeper locally. Either start OpenFaaS also locally or establish a connection to an already running instance (e.g. via port forwarding).

docker-compose:
```bash
docker-compose up kafka zookeeper
```

Port forwarding to OpenFaaS:
```bash
kubectl port-forward svc/gateway -n openfaas 31112:8080
```

## Usage

Build and push the Docker image:
```bash
docker build -t ustmico/kafka-faas-connector . && docker push ustmico/kafka-faas-connector
```

**Kubernetes:**

Deployment:
```bash
kubectl apply -f kafka-faas-connector.yml
```

**Local execution:**

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

Get the Kubernetes logs:
```bash
kubectl -n $NAMESPACE logs -f $(kubectl get pods -n $NAMESPACE --selector=run=kafka-faas-connector --output=jsonpath={.items..metadata.name})
```

Restart it by deleting the Kubernetes pod:
```bash
kubectl -n $NAMESPACE delete pod $(kubectl get pods -n $NAMESPACE --selector=run=kafka-faas-connector --output=jsonpath={.items..metadata.name})
```

Produce message to topic 'transform-request' locally:
```bash
docker exec -it kafka /bin/bash
/opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic transform-request
```

Consume topic 'transform-result' locally:
```bash
docker exec -it kafka /bin/bash
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --group mico --topic transform-result
```
