from flask import request, jsonify
from cassandra.cluster import Cluster
from datetime import datetime
class BitcoinControllers():

    cluster = Cluster(['5.135.156.86'], port=9042)
    session = cluster.connect('bitcoin_data')

    def get_bitcoin_data_history(self):
        cql = "SELECT * FROM bitcoin_prices"
        prepared = self.session.prepare(cql)
        result = self.session.execute(prepared)
        data = []

        for row in result:
            data_row = {}
            data_row['price'] = row[0]
            data_row['timestamp'] = row[1]
            data.append(data_row)
        
        sorted_data = sorted(data, key=lambda x: x['timestamp'], reverse=True)
        return jsonify(sorted_data)

    
    def get_bitcoin_stats_refreshed(self):
        cql = "SELECT * FROM bitcoin_stats_refreshed LIMIT 1"
        prepared = self.session.prepare(cql)
        result = self.session.execute(prepared)
        data = []

        for row in result:
            data_row = {}
            data_row['win_date_start'] = str(row[0])
            data_row['win_date_end'] = str(row[1])
            data_row['calculation_time'] = row[2]
            data_row['difference_rate'] = row[3]
            data_row['latest_value'] = row[4]
            data_row['start_value'] = row[5]
            data_row['variation_rate'] = row[6]
            data.append(data_row)

        return jsonify(data)
    
    def get_bitcoin_stats_history(self):
        cql = "SELECT * FROM bitcoin_stats_history"
        prepared = self.session.prepare(cql)
        result = self.session.execute(prepared)
        data = []

        for row in result:
            data_row = {}
            data_row['win_date_start'] = str(row[0])
            data_row['win_date_end'] = str(row[1])
            data_row['latest_value'] = row[2]
            data_row['start_value'] = row[3]
            data_row['difference_rate'] = row[4]
            data_row['variation_rate'] = row[5]
            data.append(data_row)

        return jsonify(data)