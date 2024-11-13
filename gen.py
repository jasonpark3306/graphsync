import psycopg2
from datetime import datetime, timedelta
import random
import time
import configparser

def load_config():
    config = configparser.ConfigParser()
    config.read('db.ini')
    return config

def connect_db(db_config):
    return psycopg2.connect(
        host=db_config['host'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        port=db_config['port']
    )

def generate_random_order():
    customer_id = random.randint(1, 100)
    order_date = datetime.now() - timedelta(days=random.randint(0, 30))
    status = random.choice(['NEW', 'PROCESSING', 'SHIPPED', 'DELIVERED'])
    total_amount = round(random.uniform(10.0, 1000.0), 2)
    
    return (customer_id, order_date, status, total_amount)

def insert_random_orders(db_config, num_orders=1, interval=2):
    conn = connect_db(db_config)
    cursor = conn.cursor()
    
    try:
        for _ in range(num_orders):
            order = generate_random_order()
            
            cursor.execute("""
                INSERT INTO orders (customer_id, order_date, status, total_amount)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, order)
            
            order_id = cursor.fetchone()[0]
            print(f"Inserted order {order_id}: {order}")
            
            conn.commit()
            
            if interval > 0:
                time.sleep(interval)  # Wait between inserts
                
    except Exception as e:
        print(f"Error inserting order: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("Loading configuration from db.ini...")
    config = load_config()
    
    if 'postgresql' not in config:
        print("Error: PostgreSQL configuration not found in db.ini")
        exit(1)
        
    db_config = config['postgresql']
    print(f"Connected to PostgreSQL at {db_config['host']}:{db_config['port']}")
    
    print("Starting random order insertion...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            insert_random_orders(db_config, 1, 2)
    except KeyboardInterrupt:
        print("\nStopped by user")