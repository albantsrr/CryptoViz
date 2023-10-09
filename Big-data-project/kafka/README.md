# Installation et lacement de Kafka et Zookeeper via docker

```
 $ sudo docker-compose up -d
```

# Création d'un topic Kafka pour produire et consommer la données

```
 $ sudo docker exec -it kafka_kafka_1 kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic bitcoin_topic
```