-- create_keyspace.cql
CREATE KEYSPACE IF NOT EXISTS bitcoin_data
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE bitcoin_data;

CREATE TABLE IF NOT EXISTS bitcoin_prices (
	id UUID PRIMARY KEY,
	price DECIMAL, 
	timestamp TIMESTAMP 
); 


