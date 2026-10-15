import json
from rate_limiter.mainclass import RateLimiter
from kafka import KafkaConsumer
import psycopg2

valid_requests = ["rate_limit_change", "rate_speed_change"]
valid_requests_data = [-1, 0, 1, 2]

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

def get_connection():
    return psycopg2.connect(host="localhost", port=5432, dbname="events", user="user", password="password")

def save_accepted(cursor, event: dict) -> None:
    cursor.execute(
        """
        INSERT INTO accepted_requests
            (request_id, user_id, request_type, request_data, status, event_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (request_id) DO NOTHING
        """,
        (event["request_id"], event["user_id"], event["request_type"],
         event["request_data"], event["status"], event["timestamp"]),
    )

def main() -> None:
    
    consumer = KafkaConsumer(
        "requests",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda request: json.loads(request.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="request-consumer-group"
    )

    conn = get_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    for message in consumer:
        event = message.value
        user = RateLimiter(id)

        if not is_valid_request(event):
            print(f"REJECTED (invalid): {event}")
            continue

        if not user.allow(id, user.bucket):
            print(f"REJECTED (rate limited): {event["user_id"]}")
            continue

        print(f"ACCEPTED: {event}")
        save_accepted(cursor, event)

        if event["request_type"] == "rate_limit_change":
            RateLimiter.adjust_limits(user, tokens_max=(event["request_data"]))
        else:
            RateLimiter.adjust_limits(user, refill_rate=(event["request_data"]))

if __name__ == "__main__":
    main()