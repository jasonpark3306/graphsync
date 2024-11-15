from dotenv import load_dotenv
import os

load_dotenv()

POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://postgres:postgres@postgres:5432/sourcedb")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
CACHE_EXPIRATION = int(os.getenv("CACHE_EXPIRATION", 3600))