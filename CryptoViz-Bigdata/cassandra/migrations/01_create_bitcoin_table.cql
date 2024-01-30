-- create_keyspace.cql
CREATE KEYSPACE IF NOT EXISTS bitcoin_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE bitcoin_data;

CREATE TABLE bitcoin_data.bitcoin_prices (
    timestamp TIMESTAMP,
    price DOUBLE,
    PRIMARY KEY (timestamp)
);



