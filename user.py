import json
import random
import time
import uuid
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

fakedata = Faker()
fakeusers = [f"user{fakedata.unique.random_number(digits=8)}" for i in range(20)]
requests = ["rate_limit_change", "rate_speed_change"]

def generate_random_request() -> dict:
    fakerequest = {
        "request_id": str(uuid.uuid4()),
        "user_id": random.choice(fakeusers),
        "request_type": random.choice(requests),
        "request_data": random.randint(-1,+2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "pending"
    }

    return fakerequest