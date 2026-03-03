from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

TOPICS = ["transactions", "predictions"]
KAFKA_BROKER = "localhost:9092"

admin_client = KafkaAdminClient(
    bootstrap_servers=KAFKA_BROKER,
    client_id='fraud-admin'
)

topics_to_create = []

for topic in TOPICS:
    topics_to_create.append(
        NewTopic(name=topic, num_partitions=1, replication_factor=1)
    )

try:
    admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
    print("✅ Topics created successfully.")
except TopicAlreadyExistsError:
    print("⚠️ Some topics already exist, skipping...")
finally:
    admin_client.close()
