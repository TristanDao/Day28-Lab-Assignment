# scripts/kafka_worker.py
import json
import time
import os
import glob
import pandas as pd
from datetime import datetime
import redis
from kafka import KafkaConsumer
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

# Setup paths
BASE_DIR = "/home/thinh/projects/vinuni/assiment/Day28-Lab-Assignment"
DELTA_LAKE_PATH = os.path.join(BASE_DIR, "delta-lake", "raw")
os.makedirs(DELTA_LAKE_PATH, exist_ok=True)

# 1. Initialize Redis (Feast Store)
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# 2. Initialize Qdrant and Recreate/Ensure Collection exists
qdrant = QdrantClient(host="localhost", port=6333)
try:
    qdrant.get_collection("documents")
    print("Qdrant collection 'documents' already exists.")
except Exception:
    print("Creating Qdrant collection 'documents'...")
    qdrant.recreate_collection(
        collection_name="documents",
        vectors_config=VectorParams(size=384, distance=Distance.COSINE)
    )

# 3. Initialize Kafka Consumer
print("Connecting to Kafka...")
while True:
    try:
        consumer = KafkaConsumer(
            "data.raw",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="smoke_worker_group",
            value_deserializer=lambda m: json.loads(m.decode())
        )
        print("Connected to Kafka successfully!")
        break
    except Exception as e:
        print(f"Waiting for Kafka to be ready: {e}")
        time.sleep(2)

# 4. Consume loop
print("Listening to topic 'data.raw'...")
for msg in consumer:
    record = msg.value
    print(f"Received record: {record}")
    
    # A. Write to Delta Lake Parquet (simulated)
    try:
        df = pd.DataFrame([record])
        filepath = os.path.join(DELTA_LAKE_PATH, f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{record.get('id', 'default')}.parquet")
        df.to_parquet(filepath)
        print(f"Saved to Delta Lake: {filepath}")
    except Exception as e:
        print(f"Failed to save to Delta Lake: {e}")

    # B. Write to Feast Redis
    try:
        feature_key = f"feature:{record.get('id', 'default')}"
        r.set(feature_key, json.dumps({
            "text": record.get("text", ""),
            "timestamp": record.get("timestamp", time.time()),
            "processed": True
        }))
        print(f"Feast Redis updated: {feature_key}")
    except Exception as e:
        print(f"Failed to update Feast Redis: {e}")

    # C. Write to Qdrant
    try:
        text = record.get("text", "")
        # Mock vector embedding
        vector = [0.1] * 384
        embed_url = os.environ.get("EMBED_NGROK_URL", "")
        if embed_url:
            try:
                import requests
                resp = requests.post(f"{embed_url}/embed", json={"texts": [text]}, timeout=5.0)
                if resp.status_code == 200:
                    vector = resp.json()["embeddings"][0]
                    print("Used remote embeddings service.")
            except Exception:
                pass
        
        point_id = abs(hash(record.get("id", "default"))) % 10000000
        qdrant.upsert(
            collection_name="documents",
            points=[PointStruct(id=point_id, vector=vector, payload=record)]
        )
        print(f"Qdrant updated: Point ID {point_id}")
    except Exception as e:
        print(f"Failed to update Qdrant: {e}")
