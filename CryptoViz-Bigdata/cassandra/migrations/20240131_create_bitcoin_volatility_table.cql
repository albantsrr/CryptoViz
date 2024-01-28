
CREATE TABLE IF NOT EXISTS bitcoin_data.bitcoin_volatility (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    price_volatility DOUBLE,
    PRIMARY KEY ((window_start, window_end))
);
