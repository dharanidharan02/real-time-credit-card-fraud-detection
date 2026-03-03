import os
import time
import json
import uuid
import logging
import paramiko
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
from pathlib import Path
from DBO.database import FraudDB

# Load environment
dotenv_path = Path(__file__).resolve().parent.parent / "sftp.env"
load_dotenv(dotenv_path=dotenv_path)

# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SFTPIngestor")

# Environment configs
SFTP_HOST = os.getenv('SFTP_HOST', 'localhost')
SFTP_PORT = int(os.getenv('SFTP_PORT', 2222))
SFTP_USER = os.getenv('SFTP_USER', 'alice')
SFTP_PASS = os.getenv('SFTP_PASS', 'password')
REMOTE_INCOMING = os.getenv('SFTP_REMOTE_INCOMING', '/incoming')
LOCAL_TMP = os.getenv('SFTP_LOCAL_TMP', './tmp/incoming')
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', 'transactions')

# Ensure local tmp folder exists
os.makedirs(LOCAL_TMP, exist_ok=True)

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# DB connector
db = FraudDB()

# SFTP
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(SFTP_HOST, SFTP_PORT, username=SFTP_USER, password=SFTP_PASS)
sftp = ssh.open_sftp()

logger.info("✅ SFTP ingestion started.")

while True:
    try:
        for fname in sftp.listdir(REMOTE_INCOMING):
            # Skip non-csv and already scored files
            if not fname.endswith(".csv"):
                continue
            if fname.endswith("_scored.csv"):
                logger.info(f"⏩ Skipping scored file: {fname}")
                continue

            remote_path = f"{REMOTE_INCOMING}/{fname}"
            local_path = os.path.join(LOCAL_TMP, fname)

            logger.info(f"⬇ Downloading: {fname}")
            sftp.get(remote_path, local_path)

            # Wait until file is properly written
            for _ in range(5):
                if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                    break
                time.sleep(1)

            if not os.path.exists(local_path):
                logger.error(f"❌ File missing after download: {local_path}")
                continue

            df = pd.read_csv(local_path)
            txn_ids = []

            # Ensure transaction_id column
            if "transaction_id" not in df.columns:
                df.insert(0, "transaction_id", "")

            for idx, row in df.iterrows():
                txn_id = row.get("transaction_id")
                if not txn_id or txn_id.strip() == "":
                    txn_id = f"{os.path.splitext(fname)[0]}_{uuid.uuid4().hex[:8]}"
                    df.at[idx, "transaction_id"] = txn_id

                txn_ids.append(txn_id)
                producer.send(TOPIC, row.to_dict())

            producer.flush()
            logger.info(f"📤 Sent {len(txn_ids)} records to Kafka.")
            logger.info("⏳ Waiting for DB predictions...")

            # Wait for predictions from DB
            timeout = time.time() + 60
            results = []
            while time.time() < timeout:
                results = db.fetch_predictions_by_ids(txn_ids)
                if results and len(results) == len(txn_ids):
                    break
                time.sleep(2)

            logger.info(f"DB returned predictions for: {len(results)} / {len(txn_ids)} transactions.")

            if not results or len(results) < len(txn_ids):
                logger.warning(f"⚠️ Some predictions are missing. Skipping enrichment for: {fname}")
                continue

            # Enrich CSV
            pred_df = pd.DataFrame(results)
            pred_df["transaction_id"] = pred_df["transaction_id"].astype(str).str.strip()
            df["transaction_id"] = df["transaction_id"].astype(str).str.strip()

            # 🚨 Check for duplicates or empties
            if df["transaction_id"].duplicated().any():
                logger.warning("⚠️ Duplicate transaction_ids found in CSV")

            if df["transaction_id"].isnull().any():
                logger.warning("⚠️ Null transaction_ids found in CSV")

            # ✅ Safe merge
            merged = df.merge(pred_df, on="transaction_id", how="left", indicator=True)

            missing = merged[merged["_merge"] == "left_only"]
            if not missing.empty:
                logger.warning(f"⚠️ {len(missing)} records not matched in predictions:")
                logger.warning(missing[["transaction_id"]].head(5).to_string(index=False))

            logger.info(f"✅ Enriched rows with prediction: {merged['is_fraud'].notna().sum()} / {len(merged)}")

            if merged['is_fraud'].isnull().any():
                logger.warning("⚠️ Some predictions are missing in merged DataFrame. Skipping archive.")
                continue

            # # Save enriched scored file locally
            # scored_name = os.path.splitext(fname)[0] + "_scored.csv"
            # scored_path = os.path.join(LOCAL_TMP, scored_name)
            # enriched.to_csv(scored_path, index=False)

              # Save enriched file
            scored_name = os.path.splitext(fname)[0] + "_scored.csv"
            scored_path = os.path.normpath(os.path.join(LOCAL_TMP, scored_name))
            merged.drop(columns=["_merge"]).to_csv(scored_path, index=False)

            # Upload enriched scored file back to /incoming (same folder)
            sftp.put(scored_path, f"{REMOTE_INCOMING}/{scored_name}")
            logger.info(f"✅ Uploaded scored file: {scored_name} back to {REMOTE_INCOMING}")

            # Cleanup local tmp
            sftp.remove(remote_path)  # Remove original (non-scored) file from SFTP
            os.remove(local_path)     # Local original csv
            os.remove(scored_path)    # Local scored csv

    except Exception as e:
        logger.error(f"💥 Error during ingestion: {e}")

    time.sleep(10)  # Poll every 60 seconds

    #         # Save enriched file
    #         scored_name = os.path.splitext(fname)[0] + "_scored.csv"
    #         scored_path = os.path.normpath(os.path.join(LOCAL_TMP, scored_name))
    #         merged.drop(columns=["_merge"]).to_csv(scored_path, index=False)


    #         # Upload to /incoming temporarily
    #         temp_remote_path = f"{REMOTE_INCOMING}/{scored_name}"
    #         with open(scored_path, 'rb') as f:
    #             sftp.putfo(f, temp_remote_path)
    #         logger.info(f"✅ Uploaded scored file to: /tmp{temp_remote_path}")

    #         # Then rename/move it inside SFTP to archive/processed folder
    #         archive_path = f"{REMOTE_ARCHIVE}/{scored_name}"
    #         try:
    #             sftp.rename(temp_remote_path, archive_path)
    #             logger.info(f"✅ Renamed to archive: {archive_path}")
    #         except Exception as rename_err:
    #             logger.error(f"💥 Failed to rename to archive: {rename_err}")

    #         # Cleanup
    #         sftp.remove(remote_path)
    #         os.remove(local_path)
    #         os.remove(scored_path)

    # except Exception as e:
    #     logger.error(f"💥 Error during ingestion: {e}")

    # time.sleep(10)  # Wait before next poll