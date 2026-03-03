# import pandas as pd
# from kafka import kafkaproducer
# import json
# import time

# kafka_topic = "transactions"
# kafka_broker = "localhost:9092"

# # load csv and prepare producer
# df = pd.read_csv("c:/users/owner/downloads/sample_transactions.csv").sample(frac=1)
# producer = kafkaproducer(
#     bootstrap_servers=kafka_broker,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# batch_size = 10
# delay = 1  # seconds between batches

# print(f"🚀 sending {len(df)} transactions in batches of {batch_size}...")

# for i in range(0, len(df), batch_size):
#     batch = df.iloc[i:i + batch_size]
#     for idx, row in batch.iterrows():
#         msg = row.to_dict()
#         msg["transaction_id"] = f"txn{idx}"
#         producer.send(kafka_topic, msg)
#         print(f"📤 sent: {msg['transaction_id']}")
#     time.sleep(delay)

# producer.flush()
# producer.close()
