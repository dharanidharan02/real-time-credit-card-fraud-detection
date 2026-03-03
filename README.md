📌 Real-Time Credit Card Fraud Detection

A production-style real-time fraud detection system that processes streaming transaction data using Kafka and evaluates fraud risk dynamically using Python-based detection logic.

🚀 Overview

This project simulates a real-time credit card transaction pipeline where:

Transactions are produced to Kafka topics

A processing service consumes and evaluates fraud signals

Suspicious transactions are flagged in real time

Monitoring and modular services handle pipeline observability

The goal is to demonstrate:

Event-driven architecture

Real-time data processing

Modular system design

Containerized deployment using Docker

🏗 Architecture
Transaction Producer
        ↓
     Kafka Topic
        ↓
Fraud Detection Service (Python)
        ↓
Fraud Alerts / Logging
        ↓
Monitoring Module
🛠 Tech Stack

Python

Apache Kafka

Docker

Docker Compose

Visual Studio (project structure)

Modular service-based design

📂 Project Structure
real-time-credit-card-fraud-detection/
│
├── app/                # Core application logic
├── kafka_processing/   # Kafka producer/consumer logic
├── monitoring/         # Monitoring and observability
├── DBO/                # Data handling / persistence layer
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .gitignore
⚙️ How to Run
1️⃣ Clone the repository
git clone https://github.com/dharanidharan02/real-time-credit-card-fraud-detection.git
cd real-time-credit-card-fraud-detection
2️⃣ Run using Docker (Recommended)
docker-compose up --build

This will:

Start Kafka

Build and run fraud detection services

Begin processing streaming data

3️⃣ Run Locally (Without Docker)

Create virtual environment:

python -m venv venv
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows

Install dependencies:

pip install -r requirements.txt

Run main service:

python main.py
🔍 Fraud Detection Logic

The system evaluates transactions based on configurable fraud signals such as:

Transaction amount thresholds

Suspicious transaction frequency

Pattern-based anomaly triggers

Rapid location changes (if simulated)

The detection engine is modular and can be extended with:

ML-based models

Feature engineering pipelines

Model inference endpoints

Risk scoring systems

📊 Future Improvements

Integrate ML-based fraud classifier

Add REST API for real-time fraud score retrieval

Add dashboard for fraud analytics

Implement Redis caching for risk scoring

Add CI/CD pipeline

🎯 Why This Project Matters

This system demonstrates:

Real-time event processing

Distributed system design

Streaming data engineering

Docker-based environment setup

Clean modular backend architecture

It reflects production-style backend engineering practices used in fintech and payment systems.

👨‍💻 Author

Dharanidharan Ekambaram
Backend & Data Engineering Enthusiast
Toronto, Canada
