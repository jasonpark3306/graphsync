from fastapi import FastAPI, HTTPException
from database import get_db_connection, init_db
from cache import get_cached_data, set_cached_data, clear_cache

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    init_db()

@app.get("/")
async def root():
    return {"message": "Cache API is running"}

@app.get("/users/{user_id}")
async def get_user(user_id: int):
    cache_key = f"user:{user_id}"
    cached_user = get_cached_data(cache_key)
    
    if cached_user:
        return {"source": "cache", "data": cached_user}
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
    user = cur.fetchone()
    
    cur.close()
    conn.close()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user_dict = dict(user)
    set_cached_data(cache_key, user_dict)
    
    return {"source": "database", "data": user_dict}

@app.get("/users")
async def get_users():
    cache_key = "all_users"
    cached_users = get_cached_data(cache_key)
    
    if cached_users:
        return {"source": "cache", "data": cached_users}
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT * FROM users")
    users = cur.fetchall()
    
    cur.close()
    conn.close()
    
    users_list = [dict(user) for user in users]
    set_cached_data(cache_key, users_list)
    
    return {"source": "database", "data": users_list}

@app.delete("/cache/{cache_key}")
async def clear_cache_endpoint(cache_key: str):
    clear_cache(cache_key)
    return {"message": f"Cache cleared for key: {cache_key}"}