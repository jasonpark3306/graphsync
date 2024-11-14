import psycopg2
from datetime import datetime, timedelta
import random
import time
import configparser
import faker

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

def create_tables(db_config):
    conn = connect_db(db_config)
    cursor = conn.cursor()
    
    try:
        # Drop existing tables in correct order
        cursor.execute("""
            DROP TABLE IF EXISTS order_items CASCADE;
            DROP TABLE IF EXISTS orders CASCADE;
            DROP TABLE IF EXISTS products CASCADE;
            DROP TABLE IF EXISTS customers CASCADE;
        """)
        
        # Create customers table
        cursor.execute("""
            CREATE TABLE customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                phone VARCHAR(20),
                address TEXT,
                city VARCHAR(50),
                country VARCHAR(50),
                postal_code VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create products table
        cursor.execute("""
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                category VARCHAR(50),
                price DECIMAL(10,2) NOT NULL CHECK (price > 0),
                stock INTEGER NOT NULL DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create orders table
        cursor.execute("""
            CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(id),
                order_date TIMESTAMP NOT NULL,
                status VARCHAR(20) CHECK (status IN ('NEW', 'PROCESSING', 'SHIPPED', 'DELIVERED')),
                payment_info VARCHAR(100),
                payment_exp VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create order_items table
        cursor.execute("""
            CREATE TABLE order_items (
                id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES orders(id),
                product_id INTEGER REFERENCES products(id),
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                price_at_time DECIMAL(10,2) NOT NULL CHECK (price_at_time > 0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        conn.commit()
        print("All tables created successfully")
    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def insert_initial_data(db_config, num_customers=100, num_products=50):
    conn = connect_db(db_config)
    generator = DataGenerator()
    
    try:
        # Insert customers
        print("Inserting customers...")
        for i in range(num_customers):
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO customers (name, email, phone, address, city, country, postal_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, generator.generate_customer())
                conn.commit()
                if (i + 1) % 10 == 0:
                    print(f"Inserted {i + 1} customers")
            except psycopg2.Error as e:
                print(f"Error inserting customer {i + 1}: {e}")
                conn.rollback()
            finally:
                cursor.close()
        
        # Insert products
        print("\nInserting products...")
        for i in range(num_products):
            cursor = conn.cursor()
            try:
                product_data = generator.generate_product()
                cursor.execute("""
                    INSERT INTO products (name, category, price, stock, is_active)
                    VALUES (%s, %s, %s, %s, %s)
                """, product_data)
                conn.commit()
                if (i + 1) % 10 == 0:
                    print(f"Inserted {i + 1} products")
            except psycopg2.Error as e:
                print(f"Error inserting product {i + 1}: {e}")
                conn.rollback()
            finally:
                cursor.close()
        
        print("\nInitial data insertion completed")
        
    except Exception as e:
        print(f"Unexpected error during data insertion: {e}")
    finally:
        conn.close()

class DataGenerator:
    def __init__(self):
        self.fake = faker.Faker()
        self.used_product_names = set()
        self.used_emails = set()
        
    def generate_customer(self):
        while True:
            email = self.fake.email()
            if email not in self.used_emails:
                self.used_emails.add(email)
                return (
                    self.fake.name(),
                    email,
                    self.fake.phone_number(),
                    self.fake.street_address(),
                    self.fake.city(),
                    self.fake.country(),
                    self.fake.postcode()
                )
    
    def generate_product(self):
        while True:
            product_name = f"{self.fake.company()} {self.fake.word()} {random.randint(1, 1000)}"
            if product_name not in self.used_product_names:
                self.used_product_names.add(product_name)
                categories = ['Electronics', 'Books', 'Clothing', 'Food', 'Home', 'Sports']
                return (
                    product_name,
                    random.choice(categories),
                    round(random.uniform(10.0, 1000.0), 2),
                    random.randint(0, 1000),
                    True
                )

    def generate_order(self, customer_id):
        statuses = ['NEW', 'PROCESSING', 'SHIPPED', 'DELIVERED']
        order_date = datetime.now() - timedelta(days=random.randint(0, 30))
        return (
            customer_id,
            order_date,
            random.choice(statuses),
            self.fake.credit_card_number(),
            self.fake.credit_card_expire()
        )
    
    def generate_order_item(self, order_id, product_id):
        return (
            order_id,
            product_id,
            random.randint(1, 5),
            round(random.uniform(10.0, 1000.0), 2)
        )

def generate_orders(db_config, num_orders=1, interval=2):
    conn = connect_db(db_config)
    generator = DataGenerator()
    
    try:
        # Get existing customer and product IDs
        customer_ids, product_ids = get_existing_ids(conn)
        
        for _ in range(num_orders):
            cursor = conn.cursor()
            try:
                # Generate order
                customer_id = random.choice(customer_ids)
                cursor.execute("""
                    INSERT INTO orders (customer_id, order_date, status, payment_info, payment_exp)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, generator.generate_order(customer_id))
                
                order_id = cursor.fetchone()[0]
                
                # Generate 1-5 order items for this order
                num_items = random.randint(1, 5)
                for _ in range(num_items):
                    product_id = random.choice(product_ids)
                    cursor.execute("""
                        INSERT INTO order_items (order_id, product_id, quantity, price_at_time)
                        VALUES (%s, %s, %s, %s)
                    """, generator.generate_order_item(order_id, product_id))
                
                conn.commit()
                print(f"Generated order {order_id} with {num_items} items")
                
                if interval > 0:
                    time.sleep(interval)
                    
            except Exception as e:
                print(f"Error generating order: {e}")
                conn.rollback()
            finally:
                cursor.close()
                
    except Exception as e:
        print(f"Fatal error in order generation: {e}")
    finally:
        conn.close()

def create_tables(db_config):
    conn = connect_db(db_config)
    cursor = conn.cursor()
    
    try:
        # Drop existing tables in correct order
        cursor.execute("""
            DROP TABLE IF EXISTS order_items CASCADE;
            DROP TABLE IF EXISTS orders CASCADE;
            DROP TABLE IF EXISTS products CASCADE;
            DROP TABLE IF EXISTS customers CASCADE;
        """)
        
        # Create customers table with longer varchar fields
        cursor.execute("""
            CREATE TABLE customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                email VARCHAR(200) UNIQUE NOT NULL,
                phone VARCHAR(50),
                address TEXT,
                city VARCHAR(100),
                country VARCHAR(100),
                postal_code VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create products table with longer varchar fields
        cursor.execute("""
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200) NOT NULL UNIQUE,
                category VARCHAR(100),
                price DECIMAL(10,2) NOT NULL CHECK (price > 0),
                stock INTEGER NOT NULL DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create orders table
        cursor.execute("""
            CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(id),
                order_date TIMESTAMP NOT NULL,
                status VARCHAR(20) CHECK (status IN ('NEW', 'PROCESSING', 'SHIPPED', 'DELIVERED')),
                payment_info VARCHAR(100),
                payment_exp VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create order_items table
        cursor.execute("""
            CREATE TABLE order_items (
                id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES orders(id),
                product_id INTEGER REFERENCES products(id),
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                price_at_time DECIMAL(10,2) NOT NULL CHECK (price_at_time > 0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        conn.commit()
        print("All tables created successfully")
    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def get_existing_ids(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM customers")
        customer_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT id FROM products")
        product_ids = [row[0] for row in cursor.fetchall()]
        
        if not customer_ids or not product_ids:
            raise ValueError("No customers or products found in database")
            
        return customer_ids, product_ids
    finally:
        cursor.close()

def generate_orders(db_config, num_orders=1, interval=2):
    conn = connect_db(db_config)
    cursor = conn.cursor()
    generator = DataGenerator()
    
    try:
        # Get existing customer and product IDs
        customer_ids, product_ids = get_existing_ids(conn)
        
        for _ in range(num_orders):
            # Generate order
            customer_id = random.choice(customer_ids)
            cursor.execute("""
                INSERT INTO orders (customer_id, order_date, status, payment_info, payment_exp)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, generator.generate_order(customer_id))
            
            order_id = cursor.fetchone()[0]
            
            # Generate 1-5 order items for this order
            num_items = random.randint(1, 5)
            for _ in range(num_items):
                product_id = random.choice(product_ids)
                cursor.execute("""
                    INSERT INTO order_items (order_id, product_id, quantity, price_at_time)
                    VALUES (%s, %s, %s, %s)
                """, generator.generate_order_item(order_id, product_id))
            
            conn.commit()
            print(f"Generated order {order_id} with {num_items} items")
            
            if interval > 0:
                time.sleep(interval)
                
    except Exception as e:
        print(f"Error generating orders: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def verify_data_exists(db_config):
    conn = connect_db(db_config)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM customers")
        customer_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM products")
        product_count = cursor.fetchone()[0]
        
        print(f"Current database state:")
        print(f"Customers: {customer_count}")
        print(f"Products: {product_count}")
        
        return customer_count > 0 and product_count > 0
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
    
    # Create tables
    create_tables(db_config)
    
    # Insert initial data
    insert_initial_data(db_config, num_customers=100, num_products=50)
    
    # Verify data exists
    if not verify_data_exists(db_config):
        print("Error: No customers or products in database. Cannot generate orders.")
        exit(1)
    
    print("Starting order generation...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            generate_orders(db_config, 1, 2)
    except KeyboardInterrupt:
        print("\nStopped by user")