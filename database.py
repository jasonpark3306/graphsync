import psycopg2
from psycopg2.extras import RealDictCursor
from config import POSTGRES_URI

def get_db_connection():
    return psycopg2.connect(POSTGRES_URI, cursor_factory=RealDictCursor)

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Create users table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Insert sample data
    cur.execute("""
        INSERT INTO users (name, email) VALUES 
        ('John Doe', 'john@example.com'),
        ('Jane Smith', 'jane@example.com')
        ON CONFLICT DO NOTHING
    """)
    
    conn.commit()
    cur.close()
    conn.close()