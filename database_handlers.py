import sqlite3
import logging
import os
import json
from tkinter import messagebox
from datetime import datetime

class DatabaseManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.setup_logging()

    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def init_monitoring_db(self):
        """Initialize SQLite database for monitoring"""
        try:
            # Delete existing database if exists
            if os.path.exists('monitoring.db'):
                os.remove('monitoring.db')
                
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            # Create tables with correct schema
            c.execute('''CREATE TABLE IF NOT EXISTS deployed_rules
                        (rule_name TEXT PRIMARY KEY,
                        source_type TEXT,
                        source_table TEXT,
                        target_label TEXT,
                        kafka_topic TEXT,
                        status TEXT,
                        last_updated TEXT)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS integration_status
                        (timestamp TEXT,
                        rule_name TEXT,
                        records_processed INTEGER,
                        success_count INTEGER,
                        error_count INTEGER)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS topic_status
                        (topic_name TEXT PRIMARY KEY,
                        message_count INTEGER,
                        last_offset INTEGER,
                        last_updated TEXT)''')
            
            conn.commit()
            conn.close()
            self.logger.info("Monitoring database initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize monitoring database: {str(e)}")
            messagebox.showerror("Error", "Failed to initialize monitoring database")

    def check_database_status(self):
        """Check if monitoring database is properly initialized"""
        try:
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            # Check if tables exist
            c.execute("""SELECT name FROM sqlite_master 
                        WHERE type='table' AND 
                        name IN ('integration_status', 'deployed_rules')""")
            
            existing_tables = set(row[0] for row in c.fetchall())
            required_tables = {'integration_status', 'deployed_rules'}
            
            conn.close()
            
            if not required_tables.issubset(existing_tables):
                self.logger.info("Reinitializing monitoring database")
                self.init_monitoring_db()
                
            return True
            
        except Exception as e:
            self.logger.error(f"Database status check failed: {str(e)}")
            return False

    def get_source_count(self, mapping):
        """Get record count from source database"""
        try:
            if mapping['source']['type'] == 'postgresql':
                import psycopg2
                config = {key: entry.get() for key, entry in self.pg_entries.items()}
                conn = psycopg2.connect(**config)
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {mapping['source']['table']}")
                count = cursor.fetchone()[0]
                conn.close()
                return count
                
            elif mapping['source']['type'] == 'mongodb':
                from pymongo import MongoClient
                config = {key: entry.get() for key, entry in self.mongo_entries.items()}
                client = MongoClient(f"mongodb://{config['host']}:{config['port']}/")
                db = client[config['database']]
                count = db[mapping['source']['table']].count_documents({})
                client.close()
                return count
                
        except Exception as e:
            self.logger.error(f"Failed to get source count: {str(e)}")
            return 0

    def get_neo4j_count(self, mapping):
        """Get node count from Neo4j"""
        try:
            from neo4j import GraphDatabase
            
            config = {key: entry.get() for key, entry in self.neo4j_entries.items()}
            driver = GraphDatabase.driver(
                config['url'],
                auth=(config['user'], config['password'])
            )
            
            with driver.session() as session:
                result = session.run(
                    f"MATCH (n:{mapping['target']['label']}) RETURN COUNT(n) as count"
                )
                count = result.single()['count']
                
            driver.close()
            return count
            
        except Exception as e:
            self.logger.error(f"Failed to get Neo4j count: {str(e)}")
            return 0

    def test_postgresql_connection(self, config):
        """Test PostgreSQL connection"""
        try:
            import psycopg2
            conn = psycopg2.connect(**config)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            conn.close()
            return True
        except Exception as e:
            self.logger.error(f"PostgreSQL connection error: {str(e)}")
            return False

    def test_mongodb_connection(self, config):
        """Test MongoDB connection"""
        try:
            from pymongo import MongoClient
            if config['host'] == 'localhost' or config['host'].startswith('127.0.0.1'):
                mongodb_url = f"mongodb://{config['host']}:{config['port']}/{config['database']}"
            else:
                import urllib.parse
                mongodb_url = f"mongodb+srv://{config['user']}:{urllib.parse.quote_plus(config['password'])}@{config['host']}/{config['database']}?retryWrites=true&w=majority"
            
            client = MongoClient(mongodb_url)
            client.server_info()
            client.close()
            return True
        except Exception as e:
            self.logger.error(f"MongoDB connection error: {str(e)}")
            return False

    def test_neo4j_connection(self, config):
        """Test Neo4j connection"""
        try:
            from neo4j import GraphDatabase
            driver = GraphDatabase.driver(
                config['url'],
                auth=(config['user'], config['password'])
            )
            with driver.session() as session:
                session.run("RETURN 1")
            driver.close()
            return True
        except Exception as e:
            self.logger.error(f"Neo4j connection error: {str(e)}")
            return False

    def load_source_tables(self, source_type, config):
        """Load available tables/collections from source database"""
        try:
            tables = []
            if source_type == "postgresql":
                import psycopg2
                conn = psycopg2.connect(**config)
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name
                """)
                tables = [row[0] for row in cursor.fetchall()]
                conn.close()
                
            elif source_type == "mongodb":
                from pymongo import MongoClient
                client = MongoClient(f"mongodb://{config['host']}:{config['port']}/")
                db = client[config['database']]
                tables = sorted(db.list_collection_names())
                client.close()

            return tables
                
        except Exception as e:
            self.logger.error(f"Failed to load source tables: {str(e)}")
            return []

    def load_source_fields(self, source_type, config, table_name):
        """Load fields from selected table/collection"""
        try:
            if source_type == "postgresql":
                import psycopg2
                conn = psycopg2.connect(**config)
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                """)
                fields = cursor.fetchall()
                conn.close()
                
            elif source_type == "mongodb":
                from pymongo import MongoClient
                client = MongoClient(f"mongodb://{config['host']}:{config['port']}/")
                db = client[config['database']]
                sample = db[table_name].find_one()
                fields = [(k, type(v).__name__) for k, v in sample.items()]
                client.close()
                
            return fields
                    
        except Exception as e:
            self.logger.error(f"Failed to load fields: {str(e)}")
            return []

    def update_sync_status(self, rule_name, records_processed, success_count, error_count):
        """Update sync status in database"""
        try:
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            c.execute('''INSERT INTO integration_status 
                        (timestamp, rule_name, records_processed, success_count, error_count)
                        VALUES (datetime('now'), ?, ?, ?, ?)''',
                    (rule_name, records_processed, success_count, error_count))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to update sync status: {str(e)}")

    def save_deployment_status(self, rule_name, source_type, source_table, target_label, kafka_topic):
        """Save deployment status to database"""
        try:
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            c.execute('''INSERT OR REPLACE INTO deployed_rules 
                        (rule_name, source_type, source_table, target_label, kafka_topic, status, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))''',
                    (rule_name, source_type, source_table, target_label, 
                     json.dumps(kafka_topic) if isinstance(kafka_topic, dict) else kafka_topic,
                     'Deployed'))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to save deployment status: {str(e)}")

    def get_deployed_rules(self):
        """Get list of deployed rules"""
        try:
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            c.execute('SELECT * FROM deployed_rules')
            rules = c.fetchall()
            
            conn.close()
            return rules
            
        except Exception as e:
            self.logger.error(f"Failed to get deployed rules: {str(e)}")
            return []

    def get_sync_status(self, rule_name=None):
        """Get sync status for specific rule or all rules"""
        try:
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            if rule_name:
                c.execute('''SELECT * FROM integration_status 
                            WHERE rule_name = ? 
                            ORDER BY timestamp DESC LIMIT 100''', 
                        (rule_name,))
            else:
                c.execute('''SELECT * FROM integration_status 
                            ORDER BY timestamp DESC LIMIT 100''')
                
            status = c.fetchall()
            conn.close()
            return status
            
        except Exception as e:
            self.logger.error(f"Failed to get sync status: {str(e)}")
            return []