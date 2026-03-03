from flask import Flask, request, jsonify
import joblib
import pandas as pd
import threading
import os

from kafka_processing.kafka_worker import FraudKafkaWorker
from DBO.database import FraudDB

app = Flask(__name__)

# Load the model bundle (only once, and correctly)
model_path = os.path.join(os.path.dirname(__file__), 'model.pkl')
bundle = joblib.load(model_path)
model = bundle["model"]
features = bundle["features"]

# Set up Kafka worker and DB
db = FraudDB()
kafka_worker = FraudKafkaWorker(model, features, db)

@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json()
    df = pd.DataFrame([data])
    for col in features:
        df[col] = df.get(col, 0)
    df = df[features]
    pred = bool(model.predict(df)[0])
    return jsonify({"fraud": pred})

@app.route('/batch_predict', methods=['POST'])
def batch_predict():
    data = request.get_json()
    df = pd.DataFrame(data)
    for col in features:
        df[col] = df.get(col, 0)
    df = df[features]
    preds = [bool(x) for x in model.predict(df)]
    return jsonify({"frauds": preds})

@app.route('/metrics')
def metrics():
    return "app_up 1\n", 200, {'Content-Type': 'text/plain; version=0.0.4'}


if __name__ == '__main__':
    threading.Thread(target=kafka_worker.run, daemon=True).start()
    app.run(host='0.0.0.0', port=5000)
