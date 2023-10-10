from kafka import KafkaConsumer

consumer = KafkaConsumer('bitcoin_topic',
                         bootstrap_servers=['172.18.0.5:9092'])

while True:
    for message in consumer:
        print (message)