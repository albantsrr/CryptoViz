from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='5.135.156.86:9092')

message = 'Hello, Kafka Test Producer!'.encode('utf-8')
producer.send('bitcoin_topic', value=message)
producer.flush()