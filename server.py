import json
from rate_limiter.mainclass import RateLimiter

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
    pass

if __name__ == "__main__":
    main()