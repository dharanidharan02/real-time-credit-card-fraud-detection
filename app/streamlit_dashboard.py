import streamlit as st
import pandas as pd
import time
from DBO.database import FraudDB

st.set_page_config(layout="wide")
st.title("📊 Real-Time Fraud Dashboard")

db = FraudDB()

with st.sidebar:
    st.header("🔎 Filters")
    search_txn_id = st.text_input("Transaction ID contains:")
    show_only_frauds = st.checkbox("Show only frauds", value=False)
    refresh_rate = st.slider("Auto-refresh every (sec)", 2, 20, 5)
    refresh_btn = st.button("🔄 Manual Refresh")

if "last_refresh_time" not in st.session_state:
    st.session_state.last_refresh_time = time.time()
should_refresh = (time.time() - st.session_state.last_refresh_time >= refresh_rate) or refresh_btn
if should_refresh:
    st.session_state.last_refresh_time = time.time()
    df = db.fetch_latest_predictions(limit=500)
    df['is_fraud'] = df['is_fraud'].apply(lambda v: 'Yes' if v.lower() in ['true','yes'] else 'No')
    if search_txn_id:
        df = df[df['transaction_id'].str.contains(search_txn_id, na=False)]
    if show_only_frauds:
        df = df[df['is_fraud']=='Yes']
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.sort_values('timestamp', ascending=False)
    st.session_state.df = df
if 'df' in st.session_state:
    df = st.session_state.df
    page_size = 20
    total_pages = max(1, (len(df)-1)//page_size+1)
    page = st.sidebar.number_input("Page", 1, total_pages, 1)
    start=(page-1)*page_size; end=start+page_size
    st.dataframe(df.iloc[start:end], use_container_width=True)
remaining = max(0, int(refresh_rate - (time.time() - st.session_state.last_refresh_time)))
st.sidebar.markdown(f"⏳ Refresh in **{remaining} seconds**")