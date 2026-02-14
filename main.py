from fastapi import FastAPI, UploadFile, File, HTTPException
from google.cloud import storage
from google.cloud import pubsub_v1
from pymongo import MongoClient
from datetime import datetime
import uuid
import os
import json

app = FastAPI()

# ---------- ENV ----------
GCS_BUCKET = os.environ["GCS_BUCKET"]
PUBSUB_TOPIC = os.environ["PUBSUB_TOPIC"]
MONGO_URI = os.environ["MONGODB_URI"]

# ---------- CLIENTS ----------
storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
mongo_client = MongoClient(MONGO_URI)
db = mongo_client.kvitta

topic_path = publisher.topic_path(
    os.environ["GCP_PROJECT"],
    PUBSUB_TOPIC
)


@app.post("/receipts/upload")
async def upload_receipt(file: UploadFile = File(...)):

    if file.content_type not in ["image/png", "image/jpeg", "image/jpg"]:
        raise HTTPException(status_code=400, detail="Invalid file type")

    file_bytes = await file.read()

    if len(file_bytes) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large")

    receipt_id = str(uuid.uuid4())
    blob_name = f"{receipt_id}.png"

    # Upload to GCS
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(file_bytes, content_type=file.content_type)

    # Insert Mongo record
    db.receipts.insert_one({
        "_id": receipt_id,
        "status": "pending",
        "blob_name": blob_name,
        "created_at": datetime.utcnow()
    })

    # Publish Pub/Sub message
    publisher.publish(
        topic_path,
        json.dumps({
            "receipt_id": receipt_id,
            "blob_name": blob_name
        }).encode("utf-8")
    )

    return {
        "receipt_id": receipt_id,
        "status": "pending"
    }


@app.get("/receipts/{receipt_id}")
def get_receipt(receipt_id: str):
    receipt = db.receipts.find_one({"_id": receipt_id})
    if not receipt:
        raise HTTPException(status_code=404, detail="Not found")

    receipt["_id"] = str(receipt["_id"])
    return receipt