from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

message = 'Hello, Kafka!'.encode('utf-8')
producer.send('bitcoin_topic', value=message)
producer.flush()