import json
import redis
from config import REDIS_HOST, REDIS_PORT, CACHE_EXPIRATION

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

def get_cached_data(key):
    data = redis_client.get(key)
    if data:
        return json.loads(data)
    return None

def set_cached_data(key, data):
    redis_client.setex(
        key,
        CACHE_EXPIRATION,
        json.dumps(data)
    )

def clear_cache(key):
    redis_client.delete(key)