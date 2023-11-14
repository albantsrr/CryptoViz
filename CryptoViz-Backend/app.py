from flask import Flask
from flask_cors import CORS
from routes.bitcoin_routes_bp import bitcoin_routes_bp


app = Flask(__name__)
app.config.from_object('config')
app.config['TIMEZONE'] = 'Europe/Paris'
CORS(app)



# Definition des blueprint des routes
app.register_blueprint(bitcoin_routes_bp, url_prefix="/api/bitcoin")

if __name__ == '__main__':
    app.run()