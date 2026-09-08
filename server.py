import json
from rate_limiter.mainclass import RateLimiter
from kafka import KafkaConsumer

valid_requests = ["rate_limit_change", "rate_speed_change"]
valid_requests_data = [-1, 0, 1, 2]

user = RateLimiter()

def is_valid_request(request: dict) -> bool:
    required_fields = {"request_id", "user_id", "request_type", "request_data", "timestamp", "status"}
    if not required_fields.issubset(request.keys()):
        return False
    if request["request_type"] not in valid_requests:
        return False
    if request["request_data"] not in valid_requests_data:
        return False
    if not request["user_id"]:
        return False
    return True

def main():
    consumer = KafkaConsumer(
        "requests",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda request: json.loads(request.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="request-consumer-group",
    )

    for message in consumer:
        event = message.value

        if not is_valid_request(event):
            print(f"REJECTED (invalid): {event}")
            continue

        if not user.allow():
            print(f"REJECTED (rate limited): {event['user_id']}")
            continue

        print(f"ACCEPTED: {event}")

if __name__ == "__main__":
    main()