from kafka import KafkaConsumer

consumer = KafkaConsumer('bitcoin_topic',
                         bootstrap_servers=['5.135.156.86:9092'])

while True:
    for message in consumer:
        print(message)
