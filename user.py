import json
from random import choice, uniform
from time import sleep
from uuid import uuid4
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

fakedata = Faker()
fakeusers = [f"user{fakedata.unique.random_number(digits=8)}" for i in range(20)]
requests = ["rate_limit_change", "rate_speed_change"]
requests_data = [-1, 0, 1, 2]


def generate_random_request() -> dict:
    fakerequest = {
        "request_id": str(uuid4()),
        "user_id": choice(fakeusers),
        "request_type": choice(requests),
        "request_data": choice(requests_data),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "pending"
    }

    return fakerequest

def main() -> None:
    requestproducer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda request: json.dumps(request).encode("utf-8")
        """
        value_serializer is required because kafka sends basic datatypes and not json
        """
    )
    try:
        while True:
            request = generate_random_request()
            requestproducer.send("requests",value=request)
            print(f"Sent: {request}")
            sleep(uniform(0.1, 1.0))

    except KeyboardInterrupt:
        pass
    
    finally:
        requestproducer.flush()
        requestproducer.close()

if __name__ == "__main__":
    main()