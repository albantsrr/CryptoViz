CREATE TABLE bitcoin_data.bitcoin_stats_refreshed (
    win_date_start TIMESTAMP,
    win_date_end TIMESTAMP,
    start_value DOUBLE,
    latest_value DOUBLE,
    calculation_time TIMESTAMP,
    difference_rate DOUBLE,
    variation_rate DOUBLE,
    PRIMARY KEY (win_date_start, win_date_end)
);
