from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
import json
from datetime import datetime

class FraudKafkaWorker:
    def __init__(self, model, features, db):
        self.model = model
        self.features = features
        self.db = db

        self.consumer = KafkaConsumer(
            "transactions",
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='fraud-scoring-group',
            enable_auto_commit=True
        )

        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def run(self):
        for msg in self.consumer:
            try:
                transaction = msg.value
                txn_id = transaction.pop("transaction_id", None)

                if not txn_id:
                    print("⚠️ Missing transaction_id, skipping.")
                    continue

                df = pd.DataFrame([transaction])

                # Ensure all model features are present
                for col in self.features:
                    if col not in df.columns:
                        df[col] = 0

                df = df[self.features]

                # Predict and store
                score = bool(self.model.predict(df)[0])
                fraud_str = 'Yes' if score else 'No'

                self.db.insert_prediction(txn_id, fraud_str)

                result = {
                    "transaction_id": txn_id,
                    "is_fraud": fraud_str,
                    "timestamp": datetime.utcnow().isoformat()
                }

                self.producer.send("predictions", result)
                print(f"[KafkaWorker] ✅ {txn_id} → {fraud_str}")

            except Exception as e:
                print(f"❌ Error processing message: {e}")
