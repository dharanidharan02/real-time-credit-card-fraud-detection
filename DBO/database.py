import psycopg2
from datetime import datetime
import pandas as pd

class FraudDB:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432",
            dbname="postgres",
            user="postgres",
            password="1302"
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def insert_prediction(self, txn_id, is_fraud):
    # Ensure it's a real boolean
        fraud_str = "Yes" if str(is_fraud).lower() in ["true", "yes", "1"] else "No"

        query = """
            INSERT INTO transaction.fraud_predictions (transaction_id, is_fraud, timestamp)
            VALUES (%s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING;
        """
        self.cursor.execute(query, (txn_id, fraud_str, datetime.utcnow()))
        self.conn.commit()



    def fetch_latest_predictions(self, limit=100):
        query = f'''
            SELECT transaction_id, is_fraud, timestamp
            FROM "transaction".fraud_predictions
            ORDER BY timestamp DESC
            LIMIT {limit}
        '''
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        df = pd.DataFrame(rows, columns=["transaction_id", "is_fraud", "timestamp"])
        df['is_fraud'] = df['is_fraud'].apply(lambda v: 'Yes' if str(v).lower() in ['true','yes','y','1'] else 'No')
        return df

    def fetch_by_transaction_id(self, txn_id):
        query = '''
            SELECT transaction_id, is_fraud, timestamp
            FROM "transaction".fraud_predictions
            WHERE transaction_id = %s
        '''
        self.cursor.execute(query, (txn_id,))
        rows = self.cursor.fetchall()
        df = pd.DataFrame(rows, columns=["transaction_id", "is_fraud", "timestamp"])
        df['is_fraud'] = df['is_fraud'].apply(lambda v: 'Yes' if str(v).lower() in ['true','yes','y','1'] else 'No')
        return df

    def fetch_predictions_by_ids(self, txn_ids):
        query = """
            SELECT transaction_id, is_fraud
            FROM transaction.fraud_predictions
            WHERE transaction_id = ANY(%s)
        """
        self.cursor.execute(query, (txn_ids,))
        rows = self.cursor.fetchall()
        return [{"transaction_id": str(row[0]), "is_fraud": row[1]} for row in rows]
        


