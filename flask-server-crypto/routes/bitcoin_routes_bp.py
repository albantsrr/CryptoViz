from flask import Blueprint
from controllers.BitcoinControllers import BitcoinControllers

Bitcoin_Controllers = BitcoinControllers()

bitcoin_routes_bp = Blueprint('bitcoin_routes_bp', __name__)
bitcoin_routes_bp.route('/get_bitcoin_data_history', methods=['GET'])(Bitcoin_Controllers.get_bitcoin_data_history)
bitcoin_routes_bp.route('/get_bitcoin_stats_refreshed', methods=['GET'])(Bitcoin_Controllers.get_bitcoin_stats_refreshed)
bitcoin_routes_bp.route('/get_bitcoin_stats_history', methods=['GET'])(Bitcoin_Controllers.get_bitcoin_stats_history)