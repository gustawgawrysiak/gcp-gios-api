from flask import Flask, jsonify, Response
from typing import Tuple
from common.gios import main as fetch_stations
import firebase_admin
from firebase_admin import firestore


def get_firebase_admin() -> firestore.client:
    # credentials = firebase_admin.credentials.Certificate('config.json')
    firebase_app = firebase_admin.initialize_app(credentials, {})
    client = firestore.client()
    return client


def get_app() -> Flask:
    flask_app = Flask(__name__)
    return flask_app


app = get_app()
fc = get_firebase_admin()


@app.route('/stations/all', methods=['GET'])
def get_all_stations() -> Tuple[Response, int]:
    ref = fc.collection(u'')
    try:
        response = ref.document('').get()
    except Exception as e:
        return jsonify({"message": "error occurred"}), 500
    return jsonify(response.to_dict()), 200


@app.route('/stations/fetch', methods=['GET'])
def fetch_all_stations_data():
    ref = fc.collection(u'').document('')
    try:
        res = fetch_stations()
        ref.set(res)
    except Exception as e:
        return jsonify({"message": "error occurred"}), 500
    return jsonify({"success": "true"}), 200


@app.route('/avg/', methods=['GET'])
def get_avg_data() -> Tuple[Response, int]:
    ref = fc.collection(u'').document('')
    try:
        response = ref.get()
    except Exception as e:
        return jsonify({"message": "error occurred"}), 500
    return jsonify(response.to_dict()), 200


if __name__ == '__main__':
    app.run()
