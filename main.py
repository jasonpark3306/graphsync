import tkinter as tk
from tkinter import ttk, messagebox

import configparser
import json
import sqlite3
import logging
import socket
import requests
import os
import glob
from datetime import datetime
import threading
from datetime import datetime
import time
from datetime import datetime, date  # Add date here
from decimal import Decimal
from uuid import UUID
import uuid

# Kafka imports
try:
    from confluent_kafka import Producer, Consumer, KafkaException, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic  # Add this line
except ImportError:
    messagebox.showerror("Import Error", 
        "confluent-kafka is not installed.\n"
        "Please install it using: pip install confluent-kafka")

class DataIntegrationIDE:
    def init_monitoring_db(self):
        """Initialize SQLite database for monitoring with enhanced action logging"""
        try:
            # Delete existing database if exists
            if os.path.exists('monitoring.db'):
                os.remove('monitoring.db')
                
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            # Create tables with enhanced schema
            
            # Deployed rules table
            c.execute('''CREATE TABLE IF NOT EXISTS deployed_rules
                        (rule_id TEXT PRIMARY KEY,
                        rule_name TEXT,
                        source_type TEXT,
                        source_table TEXT,
                        target_label TEXT,
                        kafka_topic TEXT,
                        status TEXT,
                        last_updated TEXT,
                        created_at TEXT,
                        updated_at TEXT)''')
            
            # Integration status table
            c.execute('''CREATE TABLE IF NOT EXISTS integration_status
                        (status_id TEXT PRIMARY KEY,
                        timestamp TEXT,
                        rule_name TEXT,
                        records_processed INTEGER,
                        success_count INTEGER,
                        error_count INTEGER,
                        status TEXT,
                        details TEXT)''')
            

            # Add to init_monitoring_db method:
            c.execute('''CREATE TABLE IF NOT EXISTS sync_progress
                        (rule_name TEXT PRIMARY KEY,
                        last_id INTEGER,
                        last_sync TEXT,
                        status TEXT)''')
            
            # Topic status table
            c.execute('''CREATE TABLE IF NOT EXISTS topic_status
                        (topic_id TEXT PRIMARY KEY,
                        topic_name TEXT,
                        message_count INTEGER,
                        last_offset INTEGER,
                        status TEXT,
                        last_updated TEXT)''')
            
            # Kafka action logging table
            c.execute('''CREATE TABLE IF NOT EXISTS kafka_actions
                        (action_id TEXT PRIMARY KEY,
                        action_type TEXT,
                        rule_name TEXT,
                        topic_name TEXT,
                        status TEXT,
                        timestamp TEXT,
                        details TEXT,
                        error_message TEXT)''')
            
            # Sync operations table
            c.execute('''CREATE TABLE IF NOT EXISTS sync_operations
                        (sync_id TEXT PRIMARY KEY,
                        rule_name TEXT,
                        start_time TEXT,
                        end_time TEXT,
                        status TEXT,
                        total_records INTEGER,
                        processed_records INTEGER,
                        success_count INTEGER,
                        error_count INTEGER,
                        details TEXT)''')

            # Message logs table
            c.execute('''CREATE TABLE IF NOT EXISTS message_logs
                        (log_id TEXT PRIMARY KEY,
                        timestamp TEXT,
                        topic TEXT,
                        key TEXT,
                        message_type TEXT,
                        content TEXT,
                        offset INTEGER,
                        partition INTEGER)''')
                        
            conn.commit()
            conn.close()
            self.logger.info("Monitoring database initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize monitoring database: {str(e)}")
            messagebox.showerror("Error", "Failed to initialize monitoring database")


    def log_message(self, topic, key, value, message_type="UNKNOWN", offset=None, partition=None):
        """Log message to database with enhanced information"""
        try:
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            c.execute('''INSERT INTO message_logs 
                        (log_id, timestamp, topic, key, message_type, content, offset, partition)
                        VALUES (?, datetime('now'), ?, ?, ?, ?, ?, ?)''',
                    (str(uuid.uuid4()), topic, key, message_type, value, offset, partition))
            
            conn.commit()
            conn.close()
            
            # Update log viewer if it exists
            if hasattr(self, 'log_viewer'):
                self.refresh_log_viewer(
                    self.log_viewer['kafka_tree'],
                    self.log_viewer['message_tree']
                )
                
        except Exception as e:
            self.logger.error(f"Failed to log message: {str(e)}")

    def delivery_callback(self, err, msg, rule_name):
        """Callback for message delivery confirmation"""
        if err:
            self.logger.error(f'Message delivery failed for rule {rule_name}: {err}')
            self.update_sync_status(rule_name, 0, 0, 1)
        else:
            self.logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')
            # Log the message with additional information
            try:
                message_type = "SOURCE" if "_source" in msg.topic() else "SINK"
                self.log_message(
                    msg.topic(),
                    msg.key().decode('utf-8') if msg.key() else None,
                    msg.value().decode('utf-8'),
                    message_type,
                    msg.offset(),
                    msg.partition()
                )
            except Exception as e:
                self.logger.error(f"Failed to log message: {str(e)}")
                
    def create_log_viewer(self):
        """Create a log viewer window"""
        log_window = tk.Toplevel(self.root)
        log_window.title("Integration Log Viewer")
        log_window.geometry("1000x600")

        # Create main frame
        main_frame = ttk.Frame(log_window)
        main_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Create notebook for different log views
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill='both', expand=True)

        # Create frames for different types of logs
        kafka_frame = ttk.Frame(notebook)
        message_frame = ttk.Frame(notebook)
        
        notebook.add(kafka_frame, text="Kafka Actions")
        notebook.add(message_frame, text="Messages")

        # Kafka Actions Tree
        kafka_tree = ttk.Treeview(kafka_frame, 
                                columns=("timestamp", "action", "rule", "topic", "status"),
                                show='headings')
        
        kafka_tree.heading("timestamp", text="Timestamp")
        kafka_tree.heading("action", text="Action")
        kafka_tree.heading("rule", text="Rule Name")
        kafka_tree.heading("topic", text="Topic")
        kafka_tree.heading("status", text="Status")

        kafka_scroll = ttk.Scrollbar(kafka_frame, orient="vertical", command=kafka_tree.yview)
        kafka_tree.configure(yscrollcommand=kafka_scroll.set)
        
        kafka_tree.pack(side='left', fill='both', expand=True)
        kafka_scroll.pack(side='right', fill='y')

        # Messages Tree and JSON Viewer
        paned = ttk.PanedWindow(message_frame, orient='vertical')
        paned.pack(fill='both', expand=True)

        # Upper frame for message list
        upper_frame = ttk.Frame(paned)
        message_tree = ttk.Treeview(upper_frame, 
                                columns=("timestamp", "topic", "key", "type"),
                                show='headings')
        
        message_tree.heading("timestamp", text="Timestamp")
        message_tree.heading("topic", text="Topic")
        message_tree.heading("key", text="Key")
        message_tree.heading("type", text="Type")

        message_scroll = ttk.Scrollbar(upper_frame, orient="vertical", command=message_tree.yview)
        message_tree.configure(yscrollcommand=message_scroll.set)
        
        message_tree.pack(side='left', fill='both', expand=True)
        message_scroll.pack(side='right', fill='y')
        
        paned.add(upper_frame)

        # Lower frame for JSON content
        lower_frame = ttk.Frame(paned)
        json_text = tk.Text(lower_frame, wrap=tk.WORD, height=10)
        json_text.pack(fill='both', expand=True)
        
        paned.add(lower_frame)

        # Bind selection event
        def on_message_select(event):
            selected = message_tree.selection()
            if not selected:
                return
            
            item = message_tree.item(selected[0])
            try:
                message_data = item['values'][4]  # Store JSON data in hidden column
                formatted_json = json.dumps(json.loads(message_data), indent=2)
                json_text.delete('1.0', tk.END)
                json_text.insert('1.0', formatted_json)
            except:
                json_text.delete('1.0', tk.END)
                json_text.insert('1.0', "Invalid JSON data")

        message_tree.bind('<<TreeviewSelect>>', on_message_select)

        # Add control buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill='x', pady=(10,0))

        ttk.Button(button_frame, text="Refresh", 
                command=lambda: self.refresh_log_viewer(kafka_tree, message_tree)).pack(side='left', padx=5)
        ttk.Button(button_frame, text="Clear", 
                command=lambda: self.clear_log_viewer(kafka_tree, message_tree)).pack(side='left', padx=5)

        # Store references
        self.log_viewer = {
            'window': log_window,
            'kafka_tree': kafka_tree,
            'message_tree': message_tree,
            'json_text': json_text
        }
        
        # Initial load
        self.refresh_log_viewer(kafka_tree, message_tree)

    def refresh_log_viewer(self, kafka_tree, message_tree):
        """Refresh log viewer contents"""
        # Clear existing items
        kafka_tree.delete(*kafka_tree.get_children())
        message_tree.delete(*message_tree.get_children())

        try:
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()

            # Get Kafka actions
            c.execute('''SELECT timestamp, action_type, rule_name, topic_name, status 
                        FROM kafka_actions 
                        ORDER BY timestamp DESC''')
            
            for row in c.fetchall():
                kafka_tree.insert('', 'end', values=row)

            # Get messages from message_logs table
            c.execute('''SELECT timestamp, topic, key, message_type, content 
                        FROM message_logs 
                        ORDER BY timestamp DESC''')
            
            for row in c.fetchall():
                message_tree.insert('', 'end', values=row)

            conn.close()

        except Exception as e:
            self.logger.error(f"Failed to refresh log viewer: {str(e)}")

    def clear_log_viewer(self, kafka_tree, message_tree):
        """Clear log viewer contents"""
        kafka_tree.delete(*kafka_tree.get_children())
        message_tree.delete(*message_tree.get_children())
        

    def log_kafka_action(self, action_type, rule_name, topic_name=None, status="SUCCESS", details=None, error_message=None):
        """Log Kafka-related actions to the database"""
        try:
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            action_id = str(uuid.uuid4())
            timestamp = datetime.now().isoformat()
            
            c.execute('''INSERT INTO kafka_actions 
                        (action_id, action_type, rule_name, topic_name, status, 
                        timestamp, details, error_message)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                    (action_id, action_type, rule_name, topic_name, status, 
                    timestamp, details, error_message))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Logged Kafka action: {action_type} for rule {rule_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to log Kafka action: {str(e)}")
                    
    def track_sync_operation(self, rule_name, status="STARTED", details=None):
        """Track synchronization operations in the database"""
        try:
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            if status == "STARTED":
                sync_id = str(uuid.uuid4())
                c.execute('''INSERT INTO sync_operations 
                            (sync_id, rule_name, start_time, status, 
                            total_records, processed_records, success_count, 
                            error_count, details)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (sync_id, rule_name, datetime.now().isoformat(), 
                        status, 0, 0, 0, 0, details))
                        
            else:  # Update existing sync operation
                c.execute('''UPDATE sync_operations 
                            SET status = ?,
                            end_time = ?,
                            details = ?
                            WHERE rule_name = ? 
                            AND end_time IS NULL''',
                        (status, datetime.now().isoformat(), details, rule_name))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to track sync operation: {str(e)}")
                    
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Data Integration IDE")
        self.current_step = 0
        self.config = configparser.ConfigParser()
        
        # Add thread management
        self.source_threads = {}
        self.sink_threads = {}
        self.thread_stop_flags = {}
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        self.init_monitoring_db()
        self.load_config()
        self.setup_ui()

    def start_background_threads(self, rule_name):
        """Start background threads for source and sink processing"""
        try:
            # Set stop flag for this rule
            self.thread_stop_flags[rule_name] = False
            
            # Start source thread
            source_thread = threading.Thread(
                target=self.source_thread_process,
                args=(rule_name,),
                daemon=True
            )
            self.source_threads[rule_name] = source_thread
            source_thread.start()
            
            # Start sink thread
            sink_thread = threading.Thread(
                target=self.sink_thread_process,
                args=(rule_name,),
                daemon=True
            )
            self.sink_threads[rule_name] = sink_thread
            sink_thread.start()
            
            self.logger.info(f"Started background threads for rule: {rule_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start background threads: {str(e)}")
            return False


    def source_thread_process(self, rule_name):
        """Continuous source processing thread"""
        try:
            # Load mapping configuration
            with open(f'mappings/{rule_name}.json', 'r') as f:
                mapping = json.load(f)
            
            source_topic = f"{self.kafka_entries['topic_prefix'].get()}_{rule_name}_source"
            producer = Producer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'client.id': f'source_producer_{rule_name}',
                'acks': 'all'
            })
            
            # Get or initialize last processed ID
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            c.execute('SELECT last_id FROM sync_progress WHERE rule_name = ?', (rule_name,))
            result = c.fetchone()
            last_processed_id = result[0] if result else 0
            conn.close()

            while not self.thread_stop_flags.get(rule_name, True):
                try:
                    if mapping['source']['type'] == 'postgresql':
                        import psycopg2
                        from psycopg2.extras import RealDictCursor
                        
                        config = {key: entry.get() for key, entry in self.pg_entries.items()}
                        conn = psycopg2.connect(**config)
                        cursor = conn.cursor(cursor_factory=RealDictCursor)
                        
                        # Query for new records only
                        cursor.execute(f"""
                            SELECT * FROM {mapping['source']['table']}
                            WHERE id > %s
                            ORDER BY id
                            LIMIT 1000
                        """, (last_processed_id,))
                        
                        rows = cursor.fetchall()
                        if rows:
                            for row in rows:
                                message = self.prepare_message(row)
                                if message:
                                    try:
                                        key = str(row['id'])
                                        producer.produce(
                                            source_topic,
                                            key=key.encode('utf-8'),
                                            value=json.dumps(message).encode('utf-8'),
                                            on_delivery=lambda err, msg: self.delivery_callback(err, msg, rule_name)
                                        )
                                        last_processed_id = row['id']
                                    except Exception as e:
                                        self.logger.error(f"Error producing message: {str(e)}")
                                        
                            producer.flush()
                            
                            # Update progress in database
                            conn_monitor = sqlite3.connect('monitoring.db')
                            c = conn_monitor.cursor()
                            c.execute('''INSERT OR REPLACE INTO sync_progress 
                                        (rule_name, last_id, last_sync, status)
                                        VALUES (?, ?, datetime('now'), ?)''',
                                    (rule_name, last_processed_id, 'RUNNING'))
                            conn_monitor.commit()
                            conn_monitor.close()
                        
                        conn.close()
                    
                    # Sleep before next check (adjust interval as needed)
                    time.sleep(5)  # Check every 5 seconds
                    
                except Exception as e:
                    self.logger.error(f"Error in source thread: {str(e)}")
                    time.sleep(5)  # Sleep before retry
                    
        except Exception as e:
            self.logger.error(f"Source thread failed: {str(e)}")
        finally:
            if 'producer' in locals():
                producer.flush()


    def get_sync_status(self, rule_name):
        """Get current sync status for a rule"""
        try:
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            c.execute('''SELECT last_id, last_sync, status 
                        FROM sync_progress 
                        WHERE rule_name = ?''', (rule_name,))
            result = c.fetchone()
            conn.close()
            
            if result:
                return {
                    'last_id': result[0],
                    'last_sync': result[1],
                    'status': result[2]
                }
            return None
        except Exception as e:
            self.logger.error(f"Failed to get sync status: {str(e)}")
            return None
                
    def prepare_message(self, row):
        """Prepare message from database row with type handling"""
        try:
            message = {}
            for column, value in row.items():
                if value is not None:
                    if isinstance(value, (datetime, date)):
                        value = value.isoformat()
                    elif isinstance(value, (Decimal, UUID)):
                        value = str(value)
                    elif isinstance(value, bytes):
                        value = value.hex()
                    elif isinstance(value, dict):
                        value = json.dumps(value)
                    else:
                        value = str(value)
                message[column] = value
            return message
        except Exception as e:
            self.logger.error(f"Error preparing message: {str(e)}")
            return None


    def sink_thread_process(self, rule_name):
        """Continuous sink processing thread"""
        try:
            # Load mapping configuration
            with open(f'mappings/{rule_name}.json', 'r') as f:
                mapping = json.load(f)
            
            source_topic = f"{self.kafka_entries['topic_prefix'].get()}_{rule_name}_source"
            sink_topic = f"{self.kafka_entries['topic_prefix'].get()}_{rule_name}_sink"
            
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_sink_{rule_name}",
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            })
            
            consumer.subscribe([source_topic])
            
            producer = Producer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'client.id': f'sink_producer_{rule_name}'
            })
            
            while not self.thread_stop_flags.get(rule_name, True):
                try:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    
                    if msg.error():
                        self.logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
                    # Process message
                    if self.process_sink_message(msg, mapping):
                        producer.produce(
                            sink_topic,
                            key=msg.key(),
                            value=msg.value(),
                            on_delivery=lambda err, msg: self.delivery_callback(err, msg, rule_name)
                        )
                        producer.flush()
                        consumer.commit(msg)
                        
                except Exception as e:
                    self.logger.error(f"Error in sink thread: {str(e)}")
                    time.sleep(5)  # Sleep before retry
                    
        except Exception as e:
            self.logger.error(f"Sink thread failed: {str(e)}")
        finally:
            consumer.close()
            producer.flush()

    def stop_background_threads(self, rule_name):
        """Stop background threads for a rule"""
        try:
            self.thread_stop_flags[rule_name] = True
            
            if rule_name in self.source_threads:
                self.source_threads[rule_name] = None
                
            if rule_name in self.sink_threads:
                self.sink_threads[rule_name] = None
                
            # Update status in database
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            c.execute('''UPDATE sync_progress 
                        SET status = 'STOPPED'
                        WHERE rule_name = ?''', (rule_name,))
            conn.commit()
            conn.close()
                
            self.logger.info(f"Stopped background threads for rule: {rule_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to stop background threads: {str(e)}")
            
        
    def load_config(self):
        """Load configuration from db.ini file"""
        try:
            self.config.read('db.ini')
        except Exception as e:
            messagebox.showerror("Config Error", f"Error loading db.ini: {str(e)}")
            # Create default config if file doesn't exist
            self.create_default_config()

    def create_default_config(self):
        """Create default db.ini if it doesn't exist"""
        self.config['postgresql'] = {
            'host': 'localhost',
            'port': '5432',
            'database': 'skie',
            'user': 'postgres',
            'password': 'postgres'
        }
        self.config['mongodb'] = {
            'host': 'localhost',
            'port': '27017',
            'database': 'skie',
            'user': 'admin',
            'password': 'admin'
        }
        self.config['neo4j'] = {
            'url': 'bolt://localhost:7687',
            'user': 'neo4j',
            'password': 'neo4j_password'
        }
        self.config['kafka'] = {
            'bootstrap_servers': 'localhost:9092',
            'zookeeper': 'localhost:2181',
            'schema_registry': 'http://localhost:8081',
            'connect_rest': 'http://localhost:8083',
            'topic_prefix': 'skie',
            'group_id': 'skie_group',
            'auto_offset_reset': 'earliest'
        }
        self.save_config()


    def map_all_columns(self):
        """Map all source columns to target properties with same names"""
        try:
            # Clear existing mappings
            self.mapped_columns.delete(*self.mapped_columns.get_children())
            
            # Get all source columns
            for item in self.source_columns.get_children():
                column = self.source_columns.item(item)['values'][0]
                # Insert with same name for both source and target
                self.mapped_columns.insert('', 'end', values=(column, column))
                
            self.logger.info("Mapped all columns successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to map all columns: {str(e)}")
            messagebox.showerror("Error", "Failed to map all columns")

    def map_selected_columns(self):
        """Map selected source columns to target properties"""
        selected = self.source_columns.selection()
        
        for item in selected:
            column = self.source_columns.item(item)['values'][0]
            # Check if already mapped
            existing = False
            for mapped in self.mapped_columns.get_children():
                if self.mapped_columns.item(mapped)['values'][0] == column:
                    existing = True
                    break
            
            if not existing:
                self.mapped_columns.insert('', 'end', values=(column, column))

    def remove_mapped_columns(self):
        """Remove selected mapped columns"""
        selected = self.mapped_columns.selection()
        for item in selected:
            self.mapped_columns.delete(item)

    def remove_all_mappings(self):
        """Remove all mapped columns"""
        self.mapped_columns.delete(*self.mapped_columns.get_children())



    def save_target_config(self):
        """Save Neo4j configuration to db.ini"""
        try:
            for key, entry in self.neo4j_entries.items():
                self.config['neo4j'][key] = entry.get()
            self.save_config()
            messagebox.showinfo("Success", "Neo4j configuration saved successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save Neo4j configuration: {str(e)}")

    def save_config(self):
        try:
            with open('db.ini', 'w') as configfile:
                self.config.write(configfile)
        except PermissionError:
            messagebox.showerror("Error", "Permission denied to write config file")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save config: {str(e)}")

    def setup_ui(self):
        # Main container
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(pady=10, expand=True)
   
        # In setup_ui method
        self.steps = [
            "Source Selection",
            "Target Config",
            "Kafka Config",  # Add this line
            "Mapping Rules",
            "Monitoring"
        ]

        
        self.create_steps()
        
        # Add status bar
        self.create_status_bar()

    def create_status_bar(self):
        """Create status bar with connection indicators"""
        self.status_frame = ttk.Frame(self.root)
        self.status_frame.pack(side='bottom', fill='x', padx=5, pady=2)
        
        # Source status
        source_frame = ttk.Frame(self.status_frame)
        source_frame.pack(side='left', padx=10)
        source_label = ttk.Label(source_frame, text="Source:")
        source_label.pack(side='left', padx=(0,5))
        self.source_type_label = ttk.Label(source_frame, text="Disconnected")
        self.source_type_label.pack(side='left')
        self.source_indicator = tk.Canvas(source_frame, width=12, height=12)
        self.source_indicator.pack(side='left', padx=5)
        
        # Target status
        target_frame = ttk.Frame(self.status_frame)
        target_frame.pack(side='left', padx=10)
        target_label = ttk.Label(target_frame, text="Target:")
        target_label.pack(side='left', padx=(0,5))
        self.target_type_label = ttk.Label(target_frame, text="Disconnected")
        self.target_type_label.pack(side='left')
        self.target_indicator = tk.Canvas(target_frame, width=12, height=12)  # Changed from source_frame to target_frame
        self.target_indicator.pack(side='left', padx=5)


        # Kafka status
        kafka_frame = ttk.Frame(self.status_frame)
        kafka_frame.pack(side='left', padx=10)
        kafka_label = ttk.Label(kafka_frame, text="Kafka:")
        kafka_label.pack(side='left', padx=(0,5))
        self.kafka_type_label = ttk.Label(kafka_frame, text="Disconnected")
        self.kafka_type_label.pack(side='left')
        self.kafka_indicator = tk.Canvas(kafka_frame, width=12, height=12)
        self.kafka_indicator.pack(side='left', padx=5)

        
        # Initialize indicators
        self.update_connection_status("source", False)
        self.update_connection_status("target", False)
        self.update_connection_status("kafka", False)


    def update_connection_status(self, connection_type, is_connected, db_type=None):
        color = "#2ECC71" if is_connected else "#95A5A6"  # Green if connected, gray if not
        text_color = "#2ECC71" if is_connected else "#95A5A6"  # Same colors for text
        
        if connection_type == "source":
            self.source_indicator.delete("all")
            self.source_indicator.create_oval(2, 2, 10, 10, fill=color, outline=color)
            if db_type:
                self.source_type_label.config(text=f"{db_type}", foreground=text_color)
            else:
                self.source_type_label.config(text="Disconnected", foreground=text_color)
        elif connection_type == "target":
            self.target_indicator.delete("all")
            self.target_indicator.create_oval(2, 2, 10, 10, fill=color, outline=color)
            if db_type:
                self.target_type_label.config(text=f"{db_type}", foreground=text_color)
            else:
                self.target_type_label.config(text="Disconnected", foreground=text_color)
        elif connection_type == "kafka":
            self.kafka_indicator.delete("all")
            self.kafka_indicator.create_oval(2, 2, 10, 10, fill=color, outline=color)
            if db_type:
                self.kafka_type_label.config(text=f"{db_type}", foreground=text_color)
            else:
                self.kafka_type_label.config(text="Disconnected", foreground=text_color)
                
    def validate_kafka_config(self):
        """Validate Kafka configuration"""
        required_fields = [
            'bootstrap_servers',
            'zookeeper',
            'topic_prefix',
            'group_id'
        ]
        
        for field in required_fields:
            if not self.kafka_entries[field].get().strip():
                return False, f"Missing required field: {field}"
                
        return True, "Configuration valid"

    def create_source_selection(self):
        frame = ttk.Frame(self.notebook)
        
        # Source selection label
        title_label = ttk.Label(frame, text="Select Source Database:", font=('TkDefaultFont', 10))
        title_label.pack(pady=10)
        
        # Radio buttons frame
        radio_frame = ttk.Frame(frame)
        radio_frame.pack(pady=5)
        
        self.source_var = tk.StringVar(value="postgresql")
        
        ttk.Radiobutton(radio_frame, text="PostgreSQL", 
                        variable=self.source_var, 
                        value="postgresql",
                        command=self.switch_database_panel).pack(side='left', padx=10)
        ttk.Radiobutton(radio_frame, text="MongoDB", 
                        variable=self.source_var, 
                        value="mongodb",
                        command=self.switch_database_panel).pack(side='left', padx=10)

        # Configuration frame
        self.config_frame = ttk.LabelFrame(frame, text="Database Configuration")
        self.config_frame.pack(pady=10, padx=20, fill="both", expand=True)

        # Show initial panel
        self.switch_database_panel()
        
        # Buttons frame
        button_frame = ttk.Frame(frame)
        button_frame.pack(pady=10, fill='x', padx=20)
        
        ttk.Button(button_frame, text="Test Connection", 
                command=self.test_connection).pack(side='left')
        ttk.Button(button_frame, text="Save Configuration", 
                command=self.save_source_config).pack(side='right')
        
        return frame

    def switch_database_panel(self):
        """Switch between database panels"""
        selected = self.source_var.get()
        
        # Remove all frames from config_frame
        for widget in self.config_frame.winfo_children():
            widget.destroy()
        
        # Common fields for both databases
        fields = [
            ('Host:', 'host'),
            ('Port:', 'port'),
            ('Database:', 'database'),
            ('User:', 'user'),
            ('Password:', 'password')
        ]
        
        if selected == "postgresql":
            # PostgreSQL panel
            self.postgres_frame = ttk.LabelFrame(self.config_frame, text="PostgreSQL Configuration")
            self.pg_entries = {}
            
            for i, (label, key) in enumerate(fields):
                lbl = ttk.Label(self.postgres_frame, text=label, width=10, anchor='e')
                lbl.grid(row=i, column=0, padx=(5,2), pady=5, sticky='e')
                
                entry = ttk.Entry(self.postgres_frame, width=30)
                entry.insert(0, self.config['postgresql'].get(key, ''))
                entry.grid(row=i, column=1, padx=(2,5), pady=5, sticky='w')
                self.pg_entries[key] = entry
                
            self.postgres_frame.pack(fill="both", expand=True, padx=10, pady=10)
        else:
            # MongoDB panel
            self.mongodb_frame = ttk.LabelFrame(self.config_frame, text="MongoDB Configuration")
            self.mongo_entries = {}
            
            for i, (label, key) in enumerate(fields):
                lbl = ttk.Label(self.mongodb_frame, text=label, width=10, anchor='e')
                lbl.grid(row=i, column=0, padx=(5,2), pady=5, sticky='e')
                
                entry = ttk.Entry(self.mongodb_frame, width=30)
                entry.insert(0, self.config['mongodb'].get(key, ''))
                entry.grid(row=i, column=1, padx=(2,5), pady=5, sticky='w')
                self.mongo_entries[key] = entry
                
            self.mongodb_frame.pack(fill="both", expand=True, padx=10, pady=10)



    def delete_mapping(self):
        """Delete selected mapping rule"""
        current = self.mapping_rule_var.get()
        if not current:
            messagebox.showwarning("Warning", "Please select a mapping rule to delete")
            return
            
        if messagebox.askyesno("Confirm Delete", 
                            f"Are you sure you want to delete mapping rule '{current}'?"):
            try:
                # Delete mapping file
                mapping_file = f"mappings/{current}.json"
                if os.path.exists(mapping_file):
                    os.remove(mapping_file)
                
                # Update combo box
                values = list(self.mapping_rule_combo['values'])
                values.remove(current)
                self.mapping_rule_combo['values'] = values
                self.mapping_rule_var.set('')
                
                # Clear form
                self.clear_mapping_form()
                
            except Exception as e:
                self.logger.error(f"Failed to delete mapping rule: {str(e)}")
                messagebox.showerror("Error", "Failed to delete mapping rule")


    def create_new_mapping(self):
        """Create a new mapping rule"""
        dialog = tk.Toplevel(self.root)
        dialog.title("New Mapping Rule")
        dialog.geometry("400x150")
        
        ttk.Label(dialog, text="Enter mapping rule name:").pack(pady=10)
        
        name_entry = ttk.Entry(dialog, width=40)
        name_entry.pack(pady=5)
        
        def save_new():
            name = name_entry.get().strip()
            if name:
                if name in self.mapping_rule_combo['values']:
                    messagebox.showwarning("Warning", "A mapping rule with this name already exists")
                    return
                    
                self.mapping_rule_var.set(name)
                current_values = list(self.mapping_rule_combo['values'])
                current_values.append(name)
                self.mapping_rule_combo['values'] = current_values
                self.clear_mapping_form()
                dialog.destroy()
            else:
                messagebox.showwarning("Warning", "Please enter a name")
        
        ttk.Button(dialog, text="Create", command=save_new).pack(pady=10)



      
    def connect_source(self):
        """Connect to source database and update table list"""
        try:
            # Test connection
            if self.source_var.get() == "postgresql":
                import psycopg2
                config = {key: entry.get() for key, entry in self.pg_entries.items()}
                conn = psycopg2.connect(**config)
                conn.close()
            elif self.source_var.get() == "mongodb":
                from pymongo import MongoClient
                config = {key: entry.get() for key, entry in self.mongo_entries.items()}
                client = MongoClient(f"mongodb://{config['host']}:{config['port']}/")
                client.server_info()
                client.close()

            # If connection successful, load tables and enable combobox
            tables = self.load_source_tables()
            if tables:
                self.source_table_combo['values'] = tables
                self.source_table_combo['state'] = 'readonly'  # Enable but readonly
                self.update_connection_status("source", True, self.source_var.get().upper())
                messagebox.showinfo("Success", "Connected to source database successfully!")
            else:
                raise Exception("No tables found in database")

        except Exception as e:
            self.update_connection_status("source", False)
            messagebox.showerror("Connection Error", 
                f"Failed to connect to {self.source_var.get().upper()}:\n{str(e)}")

    def deploy_to_kafka(self):
        """Deploy mapping rules to Kafka"""
        producer = None
        try:

            # Log deployment start
            self.log_kafka_action(
                action_type="DEPLOYMENT_START",
                rule_name=self.mapping_rule_var.get(),
                details="Starting deployment of mapping rules"
            )

            # First, do a comprehensive Kafka check
            if not self.test_kafka_connection_silent():
                raise Exception("Kafka connection failed")

            # Save and load mapping rules
            self.save_mappings()
            with open('mapping_rules.json', 'r') as f:
                mappings = json.load(f)

            # Validate mappings
            if not mappings.get('columns'):
                raise Exception("No columns mapped. Please map columns before deploying.")

            # Create producer with robust configuration
            producer_config = {
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'client.id': 'data_integration_deploy',
                'acks': 'all',
                'retries': 3,
                'retry.backoff.ms': 1000,
                'delivery.timeout.ms': 10000,
                'request.timeout.ms': 5000
            }

            topic = f"{self.kafka_entries['topic_prefix'].get()}_mappings"
            producer = Producer(producer_config)

            # Send mapping rules
            try:
                producer.produce(
                    topic,
                    key=f"{mappings['source']['table']}".encode('utf-8'),
                    value=json.dumps(mappings).encode('utf-8')
                )
                
                # Wait for delivery
                remaining = producer.flush(timeout=5)
                if remaining > 0:
                    raise Exception(f"Failed to flush all messages. {remaining} messages remaining.")

                # Verify the deployment
                if self.verify_kafka_mapping():
                    messagebox.showinfo("Success", 
                        f"Mapping rules deployed successfully\n"
                        f"Topic: {topic}\n"
                        f"Source: {mappings['source']['table']}\n"
                        f"Target: {mappings['target']['label']}")
                else:
                    raise Exception("Failed to verify mapping deployment")
                
                self.log_kafka_action(
                    action_type="DEPLOYMENT_SUCCESS",
                    rule_name=self.mapping_rule_var.get(),
                    topic_name=topic,
                    details=f"Successfully deployed mapping rules to topic {topic}"
                )

            except Exception as e:
                # Log deployment failure
                self.log_kafka_action(
                    action_type="DEPLOYMENT_FAILURE",
                    rule_name=self.mapping_rule_var.get(),
                    status="ERROR",
                    error_message=str(e),
                    details="Failed to deploy mapping rules"
                )
                raise Exception(f"Failed to send mapping rules: {str(e)}")

        except Exception as e:
            self.logger.error(f"Deployment failed: {str(e)}")
            messagebox.showerror("Deployment Error", 
                "Failed to deploy mapping rules:\n"
                f"Kafka is not connected. Please check your Kafka configuration and connection.\n\n"
                "Please check:\n"
                "1. Kafka connection\n"
                "2. Topic permissions\n"
                "3. Mapping configuration")
        finally:
            # Clean up producer
            if producer is not None:
                producer.flush()  # Final flush
                del producer  # Properly delete the producer instance

    def test_kafka_connection_silent(self):
        """Test Kafka connection without showing messages"""
        producer = None
        try:
            bootstrap_servers = self.kafka_entries['bootstrap_servers'].get()
            producer = Producer({
                'bootstrap.servers': bootstrap_servers,
                'socket.timeout.ms': 5000
            })
            producer.flush(timeout=5)
            return True
        except:
            return False
        finally:
            if producer is not None:
                producer.flush()
                del producer

    def verify_kafka_mapping(self):
        """Verify the mapping was properly deployed"""
        consumer = None
        try:
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_verify",
                'auto.offset.reset': 'earliest',
                'session.timeout.ms': 6000,
            })

            topic = f"{self.kafka_entries['topic_prefix'].get()}_mappings"
            consumer.subscribe([topic])

            # Try multiple times with timeout
            for _ in range(3):
                msg = consumer.poll(timeout=2.0)
                if msg and not msg.error():
                    return True

            return False

        except Exception as e:
            self.logger.error(f"Verification failed: {str(e)}")
            return False

        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except:
                    pass

    def verify_kafka_connection(self):
        """Verify Kafka connection with detailed checks"""
        try:
            bootstrap_servers = self.kafka_entries['bootstrap_servers'].get()
            
            # Test basic connectivity
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            host, port = bootstrap_servers.split(':')
            sock.settimeout(5)
            result = sock.connect_ex((host, int(port)))
            sock.close()
            
            if result != 0:
                return False, "Cannot connect to Kafka broker"
                
            # Test producer
            producer = Producer({
                'bootstrap.servers': bootstrap_servers,
                'socket.timeout.ms': 5000,
                'request.timeout.ms': 5000
            })
            
            # Try to produce a test message
            test_topic = f"{self.kafka_entries['topic_prefix'].get()}_test"
            try:
                producer.produce(test_topic, b"test")
                producer.flush(timeout=5)
            except Exception as e:
                return False, f"Failed to produce test message: {str(e)}"
            finally:
                producer.close()
                
            return True, "Connection verified"
            
        except Exception as e:
            return False, f"Connection verification failed: {str(e)}"

    def show_deployment_success(self, verification):
        """Show successful deployment status"""
        status_msg = (
            f"✓ Mapping rules deployed successfully\n\n"
            f"Topic: {verification['topic']}\n"
            f"Source: {verification['source_table']}\n"
            f"Target: {verification['target_label']}\n"
            f"Mapped Columns: {verification['column_count']}\n"
            f"Timestamp: {datetime.fromtimestamp(verification['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        # Create status window
        status_window = tk.Toplevel(self.root)
        status_window.title("Deployment Status")
        status_window.geometry("500x400")
        
        # Add status icon
        success_label = ttk.Label(status_window, text="✓", font=('TkDefaultFont', 48), foreground='green')
        success_label.pack(pady=10)
        
        # Status details
        text_widget = tk.Text(status_window, wrap=tk.WORD, padx=20, pady=10)
        text_widget.pack(fill='both', expand=True)
        text_widget.insert('1.0', status_msg)
        text_widget.configure(state='disabled')
        
        # Verification buttons
        button_frame = ttk.Frame(status_window)
        button_frame.pack(pady=10)
        
        ttk.Button(button_frame, text="Check Topic Status", 
                command=lambda: self.show_topic_status(verification['topic'])).pack(side='left', padx=5)
        
        ttk.Button(button_frame, text="View Messages", 
                command=lambda: self.show_topic_messages(verification['topic'])).pack(side='left', padx=5)
        

    def add_topic_monitoring(self):
        # Add to monitoring screen
        topic_frame = ttk.LabelFrame(frame, text="Kafka Topics Status")
        self.topic_tree = ttk.Treeview(topic_frame,
            columns=("topic", "partitions", "messages"),
            show='headings')
        # Show topic metrics

    def validate_data_flow(self):
        # Check source updates reaching Kafka
        source_messages = check_topic_messages(f"{prefix}_source")
        
        # Check if mapping rules applied
        mapping_status = check_mapping_application()
        
        # Check sink processing
        sink_messages = check_topic_messages(f"{prefix}_sink")
        
    # Show flow status
    #        
    def show_topic_status(self, topic):
        """Show detailed topic status"""

        topics = [
            f"{self.kafka_entries['topic_prefix'].get()}_source",
            f"{self.kafka_entries['topic_prefix'].get()}_sink", 
            f"{self.kafka_entries['topic_prefix'].get()}_mappings"
        ]
        
        try:
            from confluent_kafka.admin import AdminClient
            
            admin = AdminClient({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get()
            })
            
            # Get topic metadata
            metadata = admin.list_topics(timeout=10)
            topic_metadata = metadata.topics[topic]
            
            status_msg = (
                f"Topic: {topic}\n"
                f"Partitions: {len(topic_metadata.partitions)}\n"
                f"State: {'Active' if not topic_metadata.error else 'Error'}\n"
            )
            
            messagebox.showinfo("Topic Status", status_msg)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to get topic status: {str(e)}")

    def show_topic_messages(self, topic_name):
        """Show messages in topic with improved message retrieval"""
        if not topic_name:
            messagebox.showwarning("Warning", "Please select a topic to view messages")
            return
            
        try:
            # Create message viewer window
            viewer = tk.Toplevel(self.root)
            viewer.title(f"Messages in {topic_name}")
            viewer.geometry("800x600")

            # Create main frame
            main_frame = ttk.Frame(viewer)
            main_frame.pack(fill='both', expand=True, padx=10, pady=10)

            # Create message tree
            columns = ("offset", "timestamp", "key", "value")
            tree = ttk.Treeview(main_frame, columns=columns, show='headings', height=10)
            
            for col in columns:
                tree.heading(col, text=col.title())
                tree.column(col, width=150)

            # Add scrollbars
            y_scroll = ttk.Scrollbar(main_frame, orient='vertical', command=tree.yview)
            tree.configure(yscrollcommand=y_scroll.set)
            
            tree.pack(side='left', fill='both', expand=True)
            y_scroll.pack(side='right', fill='y')

            # Add JSON preview pane
            preview_frame = ttk.LabelFrame(viewer, text="Message Content")
            preview_frame.pack(fill='both', expand=True, padx=10, pady=(5, 10))
            
            preview_text = tk.Text(preview_frame, wrap=tk.WORD, height=10)
            preview_text.pack(fill='both', expand=True)

            # Create consumer with specific configuration
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_view_{int(time.time())}",  # Unique group ID
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False
            })

            # Assign specific partitions
            partitions = [TopicPartition(topic_name, 0)]  # Assuming partition 0
            consumer.assign(partitions)
            
            # Seek to beginning
            consumer.seek_to_beginning(partitions[0])
            
            messages = []
            timeout = 5.0  # 5 seconds timeout
            start_time = time.time()

            # Collect messages
            while time.time() - start_time < timeout:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                
                if msg.error():
                    self.logger.error(f"Consumer error: {msg.error()}")
                    continue
                    
                # Parse message
                try:
                    key = msg.key().decode('utf-8') if msg.key() else "None"
                    value = msg.value().decode('utf-8') if msg.value() else "None"
                    
                    # Try to pretty print JSON
                    try:
                        value_obj = json.loads(value)
                        value = json.dumps(value_obj, indent=2)
                    except:
                        pass

                    messages.append({
                        'offset': msg.offset(),
                        'timestamp': datetime.fromtimestamp(msg.timestamp()[1]/1000).strftime('%Y-%m-%d %H:%M:%S'),
                        'key': key,
                        'value': value
                    })
                except Exception as e:
                    self.logger.error(f"Error parsing message: {str(e)}")

            consumer.close()

            # Insert messages into tree
            for msg in messages:
                item_id = tree.insert('', 'end', values=(
                    msg['offset'],
                    msg['timestamp'],
                    msg['key'],
                    "Click to view"  # Placeholder for value
                ))
                # Store full value in item
                tree.set(item_id, 'value_full', msg['value'])

            # Handle selection
            def on_select(event):
                selected = tree.selection()
                if not selected:
                    return
                
                # Get the full value and display it
                item = tree.item(selected[0])
                value = tree.set(selected[0], 'value_full')
                
                preview_text.delete('1.0', tk.END)
                preview_text.insert('1.0', value)

            tree.bind('<<TreeviewSelect>>', on_select)

            # Add refresh button
            ttk.Button(viewer, text="Refresh", 
                    command=lambda: self.refresh_messages(tree, topic_name)).pack(pady=5)

            # Show message if no messages found
            if not messages:
                messagebox.showinfo("Info", "No messages found in topic")

        except Exception as e:
            self.logger.error(f"Failed to show messages: {str(e)}")
            messagebox.showerror("Error", f"Failed to show messages: {str(e)}")

    def refresh_messages(self, tree, topic_name):
        """Refresh messages in the tree"""
        # Clear existing items
        tree.delete(*tree.get_children())
        self.show_topic_messages(topic_name)
        
    def create_monitoring(self):
        frame = ttk.Frame(self.notebook)
        
        # Create main layout with three panes
        paned = ttk.PanedWindow(frame, orient='vertical')
        paned.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Top frame for deployed rules
        rules_frame = ttk.LabelFrame(paned, text="Deployed Mapping Rules")
        paned.add(rules_frame)
        
        # Rules Treeview
        self.rules_tree = ttk.Treeview(rules_frame, 
                                    columns=("rule", "source", "target", "topic", "status"),
                                    show='headings',
                                    height=6)
        self.rules_tree.heading("rule", text="Rule Name")
        self.rules_tree.heading("source", text="Source")
        self.rules_tree.heading("target", text="Target")
        self.rules_tree.heading("topic", text="Kafka Topic")
        self.rules_tree.heading("status", text="Status")
        
        # Add scrollbar to rules tree
        rules_scroll = ttk.Scrollbar(rules_frame, orient="vertical", command=self.rules_tree.yview)
        self.rules_tree.configure(yscrollcommand=rules_scroll.set)
        
        # Pack rules tree and scrollbar
        self.rules_tree.pack(side='left', fill='both', expand=True)
        rules_scroll.pack(side='right', fill='y')
        
        # Bottom frame for sync status
        status_frame = ttk.LabelFrame(paned, text="Synchronization Status")
        paned.add(status_frame)
        
        # Status Treeview
        self.status_tree = ttk.Treeview(status_frame, 
                                    columns=("timestamp", "rule", "records", "success", "errors"),
                                    show='headings',
                                    height=8)
        self.status_tree.heading("timestamp", text="Timestamp")
        self.status_tree.heading("rule", text="Rule Name")
        self.status_tree.heading("records", text="Records Processed")
        self.status_tree.heading("success", text="Success Count")
        self.status_tree.heading("errors", text="Error Count")
        
        # Add scrollbar to status tree
        status_scroll = ttk.Scrollbar(status_frame, orient="vertical", command=self.status_tree.yview)
        self.status_tree.configure(yscrollcommand=status_scroll.set)
        
        # Pack status tree and scrollbar  
        self.status_tree.pack(side='left', fill='both', expand=True)
        status_scroll.pack(side='right', fill='y')

        # Control frame
        control_frame = ttk.Frame(frame)
        control_frame.pack(fill='x', padx=10, pady=5)

        # Buttons
        ttk.Button(control_frame, text="Refresh Status", 
                command=self.refresh_monitoring).pack(side='left', padx=5)
        ttk.Button(control_frame, text="Show Topics",
                command=lambda: self.show_rule_topics(
                    self.rules_tree.item(self.rules_tree.selection()[0])['values'][0]
                    if self.rules_tree.selection() else None
                )).pack(side='left', padx=5)
        ttk.Button(control_frame, text="View Action History", 
                command=self.show_action_history).pack(side='left', padx=5)
        # Add this to the create_monitoring method, in the control_frame
        ttk.Button(control_frame, text="Show Log Viewer", 
                command=self.create_log_viewer).pack(side='left', padx=5)

        return frame


    def start_auto_refresh(self):
        """Start auto-refresh for monitoring"""
        def refresh_loop():
            if self.auto_refresh_var.get():
                self.refresh_monitoring()
            self.root.after(5000, refresh_loop)  # Refresh every 5 seconds
        
        refresh_loop()
        
    def setup_kafka_config(self):
        """Initialize Kafka configuration"""
        self.kafka_config = {
            'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
            'client.id': 'data_integration_client',
            'group.id': self.kafka_entries['group_id'].get(),
            'auto.offset.reset': self.auto_offset.get(),
            'socket.timeout.ms': 10000
        }


    def run_integration(self):
        # Initialize SQLite monitoring database
        self.init_monitoring_db()
        
        # Start Kafka consumers and producers
        self.start_kafka_streaming()
        
        # Begin monitoring
        self.start_monitoring()


    def monitor_topics(self):
        """Monitor Kafka topics and update status"""
        try:
            from confluent_kafka.admin import AdminClient
            
            admin_client = AdminClient({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get()
            })
            
            # Get topic list
            topics = admin_client.list_topics(timeout=10)
            
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            for topic_name, topic_metadata in topics.topics.items():
                if topic_name.startswith(self.kafka_entries['topic_prefix'].get()):
                    # Get message count and offset information
                    message_count = 0
                    last_offset = 0
                    
                    for partition in topic_metadata.partitions.values():
                        last_offset = max(last_offset, partition.high_watermark)
                        message_count += partition.high_watermark - partition.low_watermark
                    
                    # Update database
                    c.execute('''INSERT OR REPLACE INTO topic_status 
                                (topic_name, message_count, last_offset, last_updated)
                                VALUES (?, ?, ?, datetime('now'))''',
                            (topic_name, message_count, last_offset))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to monitor topics: {str(e)}")

    def create_monitoring(self):
        frame = ttk.Frame(self.notebook)
        
        # Create main layout with three panes
        paned = ttk.PanedWindow(frame, orient='vertical')
        paned.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Top frame for deployed rules
        rules_frame = ttk.LabelFrame(paned, text="Deployed Mapping Rules")
        paned.add(rules_frame)
        
        # Rules Treeview
        self.rules_tree = ttk.Treeview(rules_frame, 
                                    columns=("rule", "source", "target", "topic", "status"),
                                    show='headings',
                                    height=6)
        self.rules_tree.heading("rule", text="Rule Name")
        self.rules_tree.heading("source", text="Source")
        self.rules_tree.heading("target", text="Target")
        self.rules_tree.heading("topic", text="Kafka Topic")
        self.rules_tree.heading("status", text="Status")
        
        # Add scrollbar to rules tree
        rules_scroll = ttk.Scrollbar(rules_frame, orient="vertical", command=self.rules_tree.yview)
        self.rules_tree.configure(yscrollcommand=rules_scroll.set)
        
        # Pack rules tree and scrollbar
        self.rules_tree.pack(side='left', fill='both', expand=True)
        rules_scroll.pack(side='right', fill='y')
        
        # Bottom frame for sync status
        status_frame = ttk.LabelFrame(paned, text="Synchronization Status")
        paned.add(status_frame)
        
        # Status Treeview
        self.status_tree = ttk.Treeview(status_frame, 
                                    columns=("timestamp", "rule", "records", "success", "errors"),
                                    show='headings',
                                    height=8)
        self.status_tree.heading("timestamp", text="Timestamp")
        self.status_tree.heading("rule", text="Rule Name")
        self.status_tree.heading("records", text="Records Processed")
        self.status_tree.heading("success", text="Success Count")
        self.status_tree.heading("errors", text="Error Count")
        
        # Add scrollbar to status tree
        status_scroll = ttk.Scrollbar(status_frame, orient="vertical", command=self.status_tree.yview)
        self.status_tree.configure(yscrollcommand=status_scroll.set)
        
        # Pack status tree and scrollbar  
        self.status_tree.pack(side='left', fill='both', expand=True)
        status_scroll.pack(side='right', fill='y')

        # Control buttons frame
        control_frame = ttk.Frame(frame)
        control_frame.pack(fill='x', padx=10, pady=5)

        # Left side buttons
        left_buttons_frame = ttk.Frame(control_frame)
        left_buttons_frame.pack(side='left')
        
        ttk.Button(left_buttons_frame, text="Refresh Status", 
                command=self.refresh_monitoring).pack(side='left', padx=5)
        ttk.Button(left_buttons_frame, text="Show Topics",
                command=lambda: self.show_rule_topics(
                    self.rules_tree.item(self.rules_tree.selection()[0])['values'][0]
                    if self.rules_tree.selection() else None
                )).pack(side='left', padx=5)
        ttk.Button(left_buttons_frame, text="View Action History", 
                command=self.show_action_history).pack(side='left', padx=5)
        
        # Add this to the create_monitoring method, in the control_frame
        ttk.Button(control_frame, text="Show Log Viewer", 
               command=self.create_log_viewer).pack(side='left', padx=5)

        return frame

    def get_rule_topics(self, rule_name):
        """Get all topics associated with a rule"""
        try:
            prefix = self.kafka_entries['topic_prefix'].get()
            topics = {
                'source': f"{prefix}_{rule_name}_source",
                'sink': f"{prefix}_{rule_name}_sink",
                'mapping': f"{prefix}_mappings"
            }
            
            # Check if topics exist
            admin_client = AdminClient({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get()
            })
            
            existing_topics = admin_client.list_topics().topics
            
            topic_status = {}
            for topic_type, topic_name in topics.items():
                if topic_name in existing_topics:
                    metadata = existing_topics[topic_name]
                    topic_status[topic_type] = {
                        'name': topic_name,
                        'exists': True,
                        'partitions': len(metadata.partitions),
                        'error': metadata.error
                    }
                else:
                    topic_status[topic_type] = {
                        'name': topic_name,
                        'exists': False,
                        'partitions': 0,
                        'error': None
                    }
                    
            return topic_status
            
        except Exception as e:
            self.logger.error(f"Failed to get topics for rule {rule_name}: {str(e)}")
            return None
        
    def refresh_monitoring(self):
        """Refresh monitoring data"""
        try:
            # Clear existing items
            self.rules_tree.delete(*self.rules_tree.get_children())
            self.status_tree.delete(*self.status_tree.get_children())
            
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            # Get deployed rules from database
            c.execute('''SELECT rule_name, source_type, source_table, 
                        target_label, status, kafka_topic
                        FROM deployed_rules
                        WHERE status = 'Deployed'
                        ORDER BY last_updated DESC''')
            
            deployed_rules = c.fetchall()
            
            if deployed_rules:
                for rule in deployed_rules:
                    self.rules_tree.insert('', 'end', values=(
                        rule[0],  # rule_name
                        f"{rule[1]} - {rule[2]}",  # source
                        f"Neo4j - {rule[3]}",  # target
                        rule[5] if rule[5] else "Not Set",  # kafka_topic
                        rule[4]   # status
                    ))
                    
                # Get sync status
                c.execute('''SELECT timestamp, rule_name, records_processed, 
                            success_count, error_count 
                            FROM integration_status 
                            ORDER BY timestamp DESC LIMIT 100''')
                
                status_rows = c.fetchall()
                if status_rows:
                    for row in status_rows:
                        self.status_tree.insert('', 'end', values=row)
                else:
                    # Add initial status for deployed rules
                    for rule in deployed_rules:
                        self.status_tree.insert('', 'end', values=(
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            rule[0],
                            0, 0, 0
                        ))
                        
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to refresh monitoring: {str(e)}")
            messagebox.showerror("Error", "Failed to refresh monitoring data")
            
    def check_rule_deployment(self, rule_name):
        """Check if a rule is deployed to Kafka and monitoring database"""
        try:
            # Check database status first
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            c.execute('''SELECT status FROM deployed_rules 
                        WHERE rule_name = ? AND status = 'Deployed' ''', 
                    (rule_name,))
            
            db_deployed = c.fetchone() is not None
            conn.close()
            
            if not db_deployed:
                return False
                
            # Then check Kafka
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_check",
                'auto.offset.reset': 'earliest'
            })
            
            topic = f"{self.kafka_entries['topic_prefix'].get()}_mappings"
            consumer.subscribe([topic])
            
            # Try to find the rule in topic
            found = False
            start_time = time.time()
            while time.time() - start_time < 2:  # 2 second timeout
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                    
                if msg.error():
                    continue
                    
                try:
                    key = msg.key().decode('utf-8')
                    if key == rule_name:
                        found = True
                        break
                except:
                    continue
                    
            consumer.close()
            return found and db_deployed
            
        except Exception as e:
            self.logger.error(f"Failed to check rule deployment: {str(e)}")
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
        
        
    def update_status_tree(self):
        """Update status tree with recent sync results"""
        try:
            # Ensure database exists
            self.init_monitoring_db()
            
            # Clear existing items
            self.status_tree.delete(*self.status_tree.get_children())
            
            # Load status from SQLite
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            # Get recent status entries
            c.execute('''SELECT timestamp, rule_name, records_processed, 
                            success_count, error_count 
                        FROM integration_status 
                        ORDER BY timestamp DESC LIMIT 100''')
            
            rows = c.fetchall()
            
            if not rows:
                # Add placeholder text if no data
                self.status_tree.insert('', 'end', values=(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'No Data',
                    0,
                    0,
                    0
                ))
            else:
                for row in rows:
                    self.status_tree.insert('', 'end', values=row)
                
            conn.close()
            
        except sqlite3.OperationalError as e:
            self.logger.error(f"Database error: {str(e)}")
            self.init_monitoring_db()  # Try to reinitialize the database
            
        except Exception as e:
            self.logger.error(f"Failed to update status tree: {str(e)}")

    def validate_kafka_setup(self):
        """Validate entire Kafka setup including configuration and connectivity"""
        try:
            # First validate configuration
            is_valid, message = self.validate_kafka_config()
            if not is_valid:
                return False, message

            # Then check services
            services = self.check_kafka_service_status()
            
            # Build detailed status message
            status_messages = []
            if not services['zookeeper']:
                status_messages.append("Zookeeper is not accessible")
            if not services['kafka']:
                status_messages.append("Kafka broker is not accessible")
            if not services['schema_registry']:
                status_messages.append("Schema Registry is not accessible")
            if not services['connect']:
                status_messages.append("Kafka Connect is not accessible")

            if status_messages:
                return False, "\n".join(status_messages)

            return True, "Kafka setup is valid and all services are accessible"

        except Exception as e:
            return False, f"Validation failed: {str(e)}"

    def monitor_kafka_topics(self):
        """Background thread to monitor Kafka topics"""
        try:
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_monitor",
                'auto.offset.reset': 'latest'
            })
            
            # Subscribe to source and sink topics
            source_topic = f"{self.kafka_entries['topic_prefix'].get()}_source"
            sink_topic = f"{self.kafka_entries['topic_prefix'].get()}_sink"
            consumer.subscribe([source_topic, sink_topic])
            
            while self.monitoring_active:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                    
                if msg.error():
                    continue
                    
                # Process message and update status
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    self.update_sync_status(msg.topic(), data)
                except:
                    continue
                    
            consumer.close()
            
        except Exception as e:
            self.logger.error(f"Monitoring error: {str(e)}")
            self.monitoring_active = False
            self.update_monitoring_status()


    def previous_step(self):
        """Navigate to previous step"""
        if self.current_step > 0:
            self.current_step -= 1
            self.notebook.select(self.current_step)
            
            # Enable/disable navigation buttons
            self.next_button.config(state='normal')
            if self.current_step == 0:
                self.prev_button.config(state='disabled')

    def next_step(self):
        """Navigate to next step"""
        if self.current_step < len(self.steps) - 1:
            self.current_step += 1
            self.notebook.select(self.current_step)
            
            # Enable/disable navigation buttons
            self.prev_button.config(state='normal')
            if self.current_step == len(self.steps) - 1:
                self.next_button.config(state='disabled')

    def save_source_config(self):
        """Save current source configuration to db.ini"""
        selected = self.source_var.get()
        
        try:
            if selected == "postgresql":
                for key, entry in self.pg_entries.items():
                    self.config['postgresql'][key] = entry.get()
                self.save_config()
                messagebox.showinfo("Success", "PostgreSQL configuration saved successfully")
            elif selected == "mongodb":
                for key, entry in self.mongo_entries.items():
                    self.config['mongodb'][key] = entry.get()
                self.save_config()
                messagebox.showinfo("Success", "MongoDB configuration saved successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save configuration: {str(e)}")

    def create_kafka_config(self):
        frame = ttk.Frame(self.notebook)
        
        # Kafka configuration
        kafka_frame = ttk.LabelFrame(frame, text="Kafka Configuration")
        kafka_frame.pack(pady=10, padx=10, fill="x")
        
        # Create grid layout for Kafka configuration
        fields = [
            ('Bootstrap Servers:', 'bootstrap_servers'),
            ('Zookeeper:', 'zookeeper'),
            ('Schema Registry:', 'schema_registry'),
            ('Connect REST:', 'connect_rest'),
            ('Topic Prefix:', 'topic_prefix'),
            ('Group ID:', 'group_id')
        ]
        
        self.kafka_entries = {}
        for i, (label, key) in enumerate(fields):
            lbl = ttk.Label(kafka_frame, text=label, width=15, anchor='e')
            lbl.grid(row=i, column=0, padx=(5,2), pady=5, sticky='e')
            
            entry = ttk.Entry(kafka_frame, width=40)
            if 'kafka' in self.config and key in self.config['kafka']:
                entry.insert(0, self.config['kafka'][key])
            entry.grid(row=i, column=1, padx=(2,5), pady=5, sticky='w')
            self.kafka_entries[key] = entry

        # Additional Options Frame
        options_frame = ttk.LabelFrame(frame, text="Additional Settings")
        options_frame.pack(pady=10, padx=10, fill="x")
        
        # Auto offset reset
        self.auto_offset = tk.StringVar(value="earliest")
        ttk.Label(options_frame, text="Auto Offset Reset:").pack(side='left', padx=5)
        ttk.Radiobutton(options_frame, text="Earliest", variable=self.auto_offset, 
                        value="earliest").pack(side='left', padx=5)
        ttk.Radiobutton(options_frame, text="Latest", variable=self.auto_offset, 
                        value="latest").pack(side='left', padx=5)

        # Buttons frame
        button_frame = ttk.Frame(frame)
        button_frame.pack(pady=10, fill='x', padx=10)
        
        # Test connection button
        ttk.Button(button_frame, text="Test Kafka Connection", 
                command=self.test_kafka_connection).pack(side='left', padx=5)
        
        # Save configuration button
        ttk.Button(button_frame, text="Save Kafka Configuration", 
                command=self.save_kafka_config).pack(side='right', padx=5)
        

        # Add Status Check button
        status_button = ttk.Button(button_frame, text="Check Services Status",
                                command=self.show_kafka_status)
        status_button.pack(side='left', padx=5)
        ttk.Button(button_frame, text="Validate Setup", 
                command=self.show_kafka_validation).pack(side='left', padx=5)

        return frame

    def show_kafka_validation(self):
        """Show Kafka validation results"""
        is_valid, message = self.validate_kafka_setup()
        if is_valid:
            messagebox.showinfo("Validation Success", message)
        else:
            messagebox.showerror("Validation Failed", message)
            

    def save_kafka_config(self):
        """Save Kafka configuration to db.ini"""
        try:
            # Validate configuration first
            is_valid, message = self.validate_kafka_config()
            if not is_valid:
                messagebox.showerror("Configuration Error", message)
                return False

            if 'kafka' not in self.config:
                self.config['kafka'] = {}
                
            for key, entry in self.kafka_entries.items():
                self.config['kafka'][key] = entry.get()
            
            self.config['kafka']['auto_offset_reset'] = self.auto_offset.get()
            
            self.save_config()
            messagebox.showinfo("Success", "Kafka configuration saved successfully")
            return True
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save Kafka configuration: {str(e)}")
            return False

    def check_kafka_service_status(self):
        """Check if all Kafka services are running and accessible"""
        status = {
            'zookeeper': False,
            'kafka': False,
            'schema_registry': False,
            'connect': False
        }
        
        try:
            # 1. Check Zookeeper
            try:
                import socket
                zk_host, zk_port = self.kafka_entries['zookeeper'].get().split(':')
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((zk_host, int(zk_port)))
                sock.close()
                status['zookeeper'] = (result == 0)
            except Exception as e:
                self.logger.error(f"Zookeeper check failed: {str(e)}")

            # 2. Check Kafka Broker
            try:
                producer = Producer({
                    'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                    'socket.timeout.ms': 5000,
                    'request.timeout.ms': 5000
                })
                producer.flush(timeout=5)
                del producer
                status['kafka'] = True
            except Exception as e:
                self.logger.error(f"Kafka broker check failed: {str(e)}")

            # 3. Check Schema Registry
            try:
                schema_registry_url = self.kafka_entries['schema_registry'].get()
                response = requests.get(f"{schema_registry_url}/subjects", timeout=5)
                status['schema_registry'] = (response.status_code == 200)
            except Exception as e:
                self.logger.error(f"Schema Registry check failed: {str(e)}")

            # 4. Check Kafka Connect
            try:
                connect_url = self.kafka_entries['connect_rest'].get()
                response = requests.get(f"{connect_url}/connectors", timeout=5)
                status['connect'] = (response.status_code == 200)
            except Exception as e:
                self.logger.error(f"Kafka Connect check failed: {str(e)}")

            return status

        except Exception as e:
            self.logger.error(f"Error checking Kafka services: {str(e)}")
            return {
                'zookeeper': False,
                'kafka': False,
                'schema_registry': False,
                'connect': False
            }

    def show_kafka_status(self):
        """Display Kafka service status with detailed information"""
        status = self.check_kafka_service_status()
        
        status_window = tk.Toplevel(self.root)
        status_window.title("Kafka Services Status")
        status_window.geometry("400x300")
        
        # Services configuration details
        services = {
            'Zookeeper': {
                'running': status['zookeeper'],
                'url': self.kafka_entries['zookeeper'].get(),
                'description': 'Coordination service'
            },
            'Kafka Broker': {
                'running': status['kafka'],
                'url': self.kafka_entries['bootstrap_servers'].get(),
                'description': 'Message broker'
            },
            'Schema Registry': {
                'running': status['schema_registry'],
                'url': self.kafka_entries['schema_registry'].get(),
                'description': 'Schema management'
            },
            'Kafka Connect': {
                'running': status['connect'],
                'url': self.kafka_entries['connect_rest'].get(),
                'description': 'Integration framework'
            }
        }
        
        for i, (service, details) in enumerate(services.items()):
            frame = ttk.Frame(status_window)
            frame.pack(fill='x', padx=20, pady=5)
            
            # Service name and status
            ttk.Label(frame, text=f"{service}:").pack(side='left')
            
            # Status indicator (colored circle)
            status_canvas = tk.Canvas(frame, width=12, height=12)
            status_canvas.pack(side='right')
            
            color = "#2ECC71" if details['running'] else "#E74C3C"  # Green if running, red if not
            status_canvas.create_oval(2, 2, 10, 10, fill=color, outline=color)
            
            # Status text
            status_label = ttk.Label(frame, 
                text=f"{'Running' if details['running'] else 'Not Running'}")
            status_label.pack(side='right', padx=5)
            
            # URL/Configuration info
            ttk.Label(frame, text=f"URL: {details['url']}", 
                    font=('TkDefaultFont', 8)).pack(pady=(0,2))
            ttk.Label(frame, text=f"({details['description']})", 
                    font=('TkDefaultFont', 8, 'italic')).pack()
        
        # Add refresh button
        ttk.Button(status_window, text="Refresh Status", 
                command=lambda: self.refresh_status(status_window)).pack(pady=10)
        
        # Add detailed connection info
        if not all(status.values()):
            ttk.Label(status_window, 
                    text="⚠️ Some services are not running. Check configurations and ensure all services are started.",
                    wraplength=350,
                    foreground='red').pack(pady=10, padx=20)

    def refresh_status(self, status_window):
        """Refresh the status window with current service states"""
        status_window.destroy()
        self.show_kafka_status()


    def test_kafka_message(self):
        """Test Kafka by sending and receiving a test message"""
        try:
            bootstrap_servers = self.kafka_entries['bootstrap_servers'].get()
            test_topic = f"{self.kafka_entries['topic_prefix'].get()}_test"
            
            # Create producer
            producer = Producer({
                'bootstrap.servers': bootstrap_servers,
                'socket.timeout.ms': 5000
            })
            
            # Create consumer
            consumer = Consumer({
                'bootstrap.servers': bootstrap_servers,
                'group.id': f"{self.kafka_entries['group_id'].get()}_test",
                'auto.offset.reset': 'earliest',
                'socket.timeout.ms': 5000
            })
            
            # Send test message
            test_message = "test_message"
            producer.produce(test_topic, test_message.encode('utf-8'))
            producer.flush(timeout=5)
            
            # Try to consume the message
            consumer.subscribe([test_topic])
            msg = consumer.poll(timeout=5)
            
            if msg is None:
                raise ConnectionError("Could not receive test message")
            
            if msg.error():
                raise KafkaException(msg.error())
            
            received_message = msg.value().decode('utf-8')
            if received_message != test_message:
                raise ValueError("Received message doesn't match sent message")
            
            producer.close()
            consumer.close()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Kafka message test failed: {str(e)}")
            return False
    

    def test_kafka_connection(self):
        """Test Kafka connection using confluent-kafka with proper error handling"""
        try:
            # Validate configuration first
            is_valid, message = self.validate_kafka_config()
            if not is_valid:
                messagebox.showerror("Configuration Error", message)
                return False

            from confluent_kafka import Producer, Consumer
            
            bootstrap_servers = self.kafka_entries['bootstrap_servers'].get()
            self.logger.info(f"Testing Kafka connection to {bootstrap_servers}")
            
            # First test basic socket connection
            host, port = bootstrap_servers.split(':')
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, int(port)))
            sock.close()
                
            if result != 0:
                self.update_connection_status("kafka", False)
                raise ConnectionError(f"Cannot connect to Kafka broker at {bootstrap_servers}")

            # Test Producer with timeout
            producer_config = {
                'bootstrap.servers': bootstrap_servers,
                'socket.timeout.ms': 5000,
                'message.timeout.ms': 5000
            }
            
            producer = Producer(producer_config)
            
            # Use flush with timeout to ensure connection
            flush_result = producer.flush(timeout=5)
            if flush_result > 0:
                raise ConnectionError("Failed to flush producer queue")
                
            # Properly clean up the producer using del instead of close
            del producer
            
            # Test Consumer
            consumer_config = {
                'bootstrap.servers': bootstrap_servers,
                'group.id': self.kafka_entries['group_id'].get(),
                'auto.offset.reset': self.auto_offset.get(),
                'socket.timeout.ms': 5000,
                'session.timeout.ms': 6000
            }
            
            consumer = Consumer(consumer_config)
            # Try to list topics to verify connection
            topics = consumer.list_topics(timeout=5)
            if not topics:
                raise ConnectionError("Could not retrieve topic list from broker")
            consumer.close()
            
            self.update_connection_status("kafka", True, "Connected")
            messagebox.showinfo("Success", "Kafka connection successful!")
                
        except ImportError:
            self.update_connection_status("kafka", False)
            messagebox.showerror("Error", 
                "Please install confluent-kafka:\n"
                "pip install confluent-kafka")
        
        except ConnectionError as e:
            self.update_connection_status("kafka", False)
            messagebox.showerror("Connection Error", 
                f"Failed to connect to Kafka:\n{str(e)}\n\n"
                "Please check if:\n"
                "1. Kafka server is running\n"
                "2. The broker address is correct\n"
                "3. No firewall is blocking the connection")
        
        except Exception as e:
            error_msg = f"Failed to connect to Kafka"
            self.logger.error(f"{error_msg}: {str(e)}")
            self.update_connection_status("kafka", False)
            messagebox.showerror("Connection Error", f"{error_msg}:\n{str(e)}")

                
    def create_target_config(self):
        frame = ttk.Frame(self.notebook)
        
        # Neo4j configuration
        neo4j_frame = ttk.LabelFrame(frame, text="Neo4j Configuration")
        neo4j_frame.pack(pady=10, padx=10, fill="x")
        
        # Create grid layout for better alignment
        fields = [
            ('URL:', 'url'),
            ('User:', 'user'),
            ('Password:', 'password')
        ]
        
        self.neo4j_entries = {}
        for i, (label, key) in enumerate(fields):
            lbl = ttk.Label(neo4j_frame, text=label, width=10, anchor='e')
            lbl.grid(row=i, column=0, padx=(5,2), pady=5, sticky='e')
            
            entry = ttk.Entry(neo4j_frame, width=30)
            entry.insert(0, self.config['neo4j'].get(key, ''))
            entry.grid(row=i, column=1, padx=(2,5), pady=5, sticky='w')
            self.neo4j_entries[key] = entry

        # Buttons frame
        button_frame = ttk.Frame(frame)
        button_frame.pack(pady=10, fill='x', padx=10)
        
        # Test connection button
        ttk.Button(button_frame, text="Test Connection", 
                command=self.test_neo4j_connection).pack(side='left', padx=5)
        
        # Save configuration button
        ttk.Button(button_frame, text="Save Configuration", 
                command=self.save_target_config).pack(side='right', padx=5)
        
        return frame

    def test_neo4j_connection(self):
        """Test Neo4j connection"""
        try:
            from neo4j import GraphDatabase
            
            # Get current values from entries
            config = {key: entry.get() for key, entry in self.neo4j_entries.items()}
            
            # Test Neo4j connection
            driver = GraphDatabase.driver(
                config['url'],
                auth=(config['user'], config['password'])
            )
            with driver.session() as session:
                session.run("RETURN 1")  # Simple test query
            driver.close()
            self.update_connection_status("target", True, "Neo4j")  # Add this line
            messagebox.showinfo("Success", "Neo4j connection successful!")
            
        except Exception as e:
            self.update_connection_status("target", False)
            messagebox.showerror("Connection Error", f"Failed to connect to Neo4j: {str(e)}")
            
    def test_connection(self):
        """Test database connection based on selected source"""
        selected = self.source_var.get()
        
        try:
            if selected == "postgresql":
                import psycopg2
                config = {key: entry.get() for key, entry in self.pg_entries.items()}
                self.logger.info(f"Attempting PostgreSQL connection to {config['host']}:{config['port']}")
                conn = psycopg2.connect(
                    host=config['host'],
                    port=config['port'],
                    database=config['database'],
                    user=config['user'],
                    password=config['password']
                )
                conn.close()
                self.update_connection_status("source", True, "PostgreSQL")
                self.logger.info("PostgreSQL connection successful!")
                messagebox.showinfo("Success", "PostgreSQL connection successful!")
                
            elif selected == "mongodb":
                from pymongo import MongoClient
                config = {key: entry.get() for key, entry in self.mongo_entries.items()}
                
                # Determine MongoDB URL based on host
                if config['host'] == 'localhost' or config['host'].startswith('127.0.0.1'):
                    mongodb_url = f"mongodb://{config['host']}:{config['port']}/{config['database']}"
                    self.logger.info(f"Connecting to local MongoDB at: {config['host']}:{config['port']}")
                else:
                    import urllib.parse
                    # For remote MongoDB Atlas connection
                    mongodb_url = f"mongodb+srv://{config['user']}:{urllib.parse.quote_plus(config['password'])}@{config['host']}/{config['database']}?retryWrites=true&w=majority"
                    self.logger.info(f"Connecting to remote MongoDB at: {config['host']}")

                self.logger.debug(f"MongoDB URL: {mongodb_url}")
                
                try:
                    client = MongoClient(mongodb_url)
                    # Force connection to verify
                    client.server_info()
                    self.logger.info("MongoDB connection successful!")
                    client.close()
                    self.update_connection_status("source", True, "MongoDB")
                    messagebox.showinfo("Success", "MongoDB connection successful!")
                    
                except Exception as mongo_err:
                    error_msg = str(mongo_err)
                    self.logger.error(f"MongoDB Connection Error: {error_msg}")
                    
                    if "Authentication failed" in error_msg:
                        self.logger.error("Authentication failed - Check username and password")
                    elif "ServerSelectionTimeoutError" in error_msg:
                        self.logger.error("Server selection timeout - Check if MongoDB server is running")
                    elif "ConnectionError" in error_msg:
                        self.logger.error("Connection error - Check if MongoDB server is accessible")
                    
                    messagebox.showerror("Connection Error", f"Failed to connect to MONGODB:\n{error_msg}")
                    
        except ImportError as e:
            error_msg = f"Required database driver not installed for {selected}"
            self.logger.error(f"{error_msg}: {str(e)}")
            messagebox.showerror("Error", f"{error_msg}.\nError: {str(e)}")
        except Exception as e:
            error_msg = f"Failed to connect to {selected.upper()}"
            self.logger.error(f"{error_msg}: {str(e)}")
            self.update_connection_status("source", False)
            messagebox.showerror("Connection Error", f"{error_msg}:\n{str(e)}")

   
        
    def load_source_fields(self, event=None):
        """Load fields from selected table/collection"""
        try:
            # Clear existing items
            for item in self.source_columns.get_children():  # Changed from source_fields to source_columns
                self.source_columns.delete(item)
                
            selected_table = self.source_table_var.get()
            
            if self.source_var.get() == "postgresql":
                import psycopg2
                config = {key: entry.get() for key, entry in self.pg_entries.items()}
                conn = psycopg2.connect(**config)
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{selected_table}'
                """)
                fields = cursor.fetchall()
                conn.close()
                
            elif self.source_var.get() == "mongodb":
                from pymongo import MongoClient
                config = {key: entry.get() for key, entry in self.mongo_entries.items()}
                client = MongoClient(f"mongodb://{config['host']}:{config['port']}/")
                db = client[config['database']]
                # Get sample document to infer schema
                sample = db[selected_table].find_one()
                fields = [(k, type(v).__name__) for k, v in sample.items()]
                client.close()
                
            # Insert fields into source_columns treeview
            for field, dtype in fields:
                self.source_columns.insert('', 'end', values=(field, dtype))
                    
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load fields: {str(e)}")
                
    def add_mapping_rule(self):
        """Add new mapping rule"""
        selected = self.source_fields.selection()
        if not selected:
            messagebox.showwarning("Warning", "Please select a source field")
            return
            
        # Create mapping dialog
        dialog = tk.Toplevel(self.root)
        dialog.title("Add Mapping Rule")
        dialog.geometry("400x300")
        
        # Source field
        source_field = self.source_fields.item(selected[0])['values'][0]
        ttk.Label(dialog, text=f"Source Field: {source_field}").pack(pady=5)
        
        # Target field
        ttk.Label(dialog, text="Target Field:").pack(pady=5)
        target_field = ttk.Entry(dialog)
        target_field.pack(pady=5)
        
        # Transformation
        ttk.Label(dialog, text="Transformation (optional):").pack(pady=5)
        transformation = ttk.Entry(dialog)
        transformation.pack(pady=5)

        def save_rule():
            """Inner function to save the mapping rule"""
            self.mapping_rules.insert('', 'end', values=(
                source_field,
                target_field.get(),
                transformation.get()
            ))
            dialog.destroy()
        
        # Save button
        ttk.Button(dialog, text="Save", command=save_rule).pack(pady=10)

    def remove_mapping_rule(self):
        """Remove selected mapping rule"""
        selected = self.mapping_rules.selection()
        if selected:
            self.mapping_rules.delete(selected)


    def update_target_options(self):
        """Update target options based on selected type (Node/Relationship)"""
        selected_type = self.target_type_var.get()
        
        if selected_type == "Relationship":
            # Create dialog for relationship properties
            dialog = tk.Toplevel(self.root)
            dialog.title("Relationship Configuration")
            dialog.geometry("400x300")
            
            # Source node label
            ttk.Label(dialog, text="Source Node Label:").pack(pady=5)
            source_label = ttk.Entry(dialog)
            source_label.pack(pady=5)
            
            # Target node label
            ttk.Label(dialog, text="Target Node Label:").pack(pady=5)
            target_label = ttk.Entry(dialog)
            target_label.pack(pady=5)
            
            # Relationship type
            ttk.Label(dialog, text="Relationship Type:").pack(pady=5)
            rel_type = ttk.Entry(dialog)
            rel_type.pack(pady=5)
            
            def save_relationship_config():
                self.relationship_config = {
                    'source_label': source_label.get(),
                    'target_label': target_label.get(),
                    'relationship_type': rel_type.get()
                }
                # Update target label entry with relationship type
                self.target_label_entry.delete(0, tk.END)
                self.target_label_entry.insert(0, rel_type.get())
                dialog.destroy()
                
            ttk.Button(dialog, text="Save", command=save_relationship_config).pack(pady=10)
            
        else:  # Node type
            # Clear any existing relationship configuration
            if hasattr(self, 'relationship_config'):
                delattr(self, 'relationship_config')
            
            # Reset target label entry if it was showing a relationship type
            self.target_label_entry.delete(0, tk.END)


    def load_selected_mapping(self, event=None):
        """Load selected mapping rule"""
        current = self.mapping_rule_var.get()
        if not current:
            return
            
        try:
            mapping_file = f"mappings/{current}.json"
            if os.path.exists(mapping_file):
                with open(mapping_file, 'r') as f:
                    mapping = json.load(f)
                    
                # Update form with mapping data
                self.source_table_var.set(mapping['source']['table'])
                self.target_label_entry.delete(0, tk.END)
                self.target_label_entry.insert(0, mapping['target']['label'])
                
                # Clear existing mappings
                self.mapped_columns.delete(*self.mapped_columns.get_children())
                
                # Load mapped columns
                for source_col, target_col in mapping['columns'].items():
                    self.mapped_columns.insert('', 'end', values=(source_col, target_col))
                    
                # Load source fields for the selected table
                self.load_source_fields()
                
        except Exception as e:
            self.logger.error(f"Failed to load mapping rule: {str(e)}")
            messagebox.showerror("Error", "Failed to load mapping rule")

    def create_steps(self):
        """Create notebook tabs for each integration step"""
        # Create frames for each step
        self.frames = {}
        # In create_steps method
        self.frames["Source Selection"] = self.create_source_selection()
        self.frames["Target Config"] = self.create_target_config()
        self.frames["Kafka Config"] = self.create_kafka_config()
        self.frames["Mapping Rules"] = self.create_mapping_rules()
        self.frames["Monitoring"] = self.create_monitoring()
        
        # Add frames to notebook
        for step in self.steps:
            self.notebook.add(self.frames[step], text=step)
        
        # Navigation buttons frame
        nav_frame = ttk.Frame(self.root)
        nav_frame.pack(pady=5, fill='x')
        
        # Previous button
        self.prev_button = ttk.Button(nav_frame, 
                                    text="Previous", 
                                    command=self.previous_step)
        self.prev_button.pack(side='left', padx=5)
        
        # Next button
        self.next_button = ttk.Button(nav_frame, 
                                    text="Next", 
                                    command=self.next_step)
        self.next_button.pack(side='right', padx=5)
        
        # Initially disable previous button
        self.prev_button.config(state='disabled')
        
        # Add binding for tab changes
        self.notebook.bind('<<NotebookTabChanged>>', self.on_tab_changed)

    def on_tab_changed(self, event=None):
        """Handle tab changes"""
        current_tab = self.notebook.select()
        tab_index = self.notebook.index(current_tab)
        
        # Enable/disable navigation buttons based on current tab
        if tab_index == 0:  # First tab
            self.prev_button.config(state='disabled')
            self.next_button.config(state='normal')
        elif tab_index == len(self.steps) - 1:  # Last tab (Monitoring)
            self.prev_button.config(state='normal')
            self.next_button.config(state='disabled')
        else:  # Middle tabs
            self.prev_button.config(state='normal')
            self.next_button.config(state='normal')
                
    def on_table_selected(self, event=None):
        """Handle table selection and automatically set Neo4j label if auto-label is enabled"""
        selected_table = self.source_table_var.get()
        if not selected_table:
            return
                
        # Only auto-set label if checkbox is checked
        if self.auto_label_var.get():
            self.target_label_entry.delete(0, tk.END)
            self.target_label_entry.insert(0, selected_table.title())  # Capitalize table name
            
        # Load the fields for the selected table
        self.load_source_fields()

        # Enable mapping buttons if not already enabled
        if hasattr(self, 'mapped_columns'):
            for item in self.mapped_columns.get_children():
                self.mapped_columns.delete(item)

        # Update UI state if needed
        if hasattr(self, 'source_columns'):
            self.load_source_fields()

        # Log the selection
        self.logger.info(f"Selected source table: {selected_table}")

    def create_mapping_rules(self):
        frame = ttk.Frame(self.notebook)
        
        # Main container
        mapping_frame = ttk.LabelFrame(frame, text="Source to Target Mapping")
        mapping_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Mapping Rule Selection Frame
        rule_select_frame = ttk.Frame(mapping_frame)
        rule_select_frame.pack(fill='x', padx=5, pady=5)
        
        ttk.Label(rule_select_frame, text="Mapping Rule:").pack(side='left', padx=5)
        
        # Initialize variables
        self.mapping_rule_var = tk.StringVar()
        self.source_table_var = tk.StringVar()
        self.auto_label_var = tk.BooleanVar(value=True)
        
        # Mapping rule selector
        self.mapping_rule_combo = ttk.Combobox(rule_select_frame, 
                                            textvariable=self.mapping_rule_var,
                                            width=40)
        self.mapping_rule_combo.pack(side='left', padx=5)
        self.mapping_rule_combo.bind('<<ComboboxSelected>>', self.load_selected_mapping)
        
        # Mapping rule management buttons
        ttk.Button(rule_select_frame, text="New", 
                command=self.create_new_mapping).pack(side='left', padx=2)
        ttk.Button(rule_select_frame, text="Delete", 
                command=self.delete_mapping).pack(side='left', padx=2)
            

        # Source selection frame
        source_frame = ttk.LabelFrame(mapping_frame, text="Source")
        source_frame.pack(fill='x', padx=5, pady=5)

        # Source connection frame
        source_conn_frame = ttk.Frame(source_frame)
        source_conn_frame.pack(fill='x', padx=5, pady=2)

        # Show source type
        source_type = self.source_var.get().upper()
        ttk.Label(source_conn_frame, text=f"Source Type: {source_type}").pack(side='left', padx=5)

        # Add Connect button
        ttk.Button(source_conn_frame, text="Connect", 
                command=self.connect_source).pack(side='right', padx=5)

        # Source table selector frame
        table_frame = ttk.Frame(source_frame)
        table_frame.pack(fill='x', padx=5, pady=2)

        # Source table/collection selector
        ttk.Label(table_frame, text="Table/Collection:").pack(side='left', padx=5)
        self.source_table_combo = ttk.Combobox(table_frame, 
                                            textvariable=self.source_table_var, 
                                            width=30,
                                            state='disabled')  # Initially disabled
        self.source_table_combo.pack(side='left', padx=5)
        self.source_table_combo.bind('<<ComboboxSelected>>', self.on_table_selected)

        
        # Target frame
        target_frame = ttk.LabelFrame(mapping_frame, text="Target (Neo4j)")
        target_frame.pack(fill='x', padx=5, pady=5)
        
        # Label entry
        ttk.Label(target_frame, text="Node Label:").pack(side='left', padx=5)
        self.target_label_entry = ttk.Entry(target_frame, width=30)
        self.target_label_entry.pack(side='left', padx=5)
        
        # Auto-label checkbox
        ttk.Checkbutton(target_frame, 
                        text="Auto-set label from source",
                        variable=self.auto_label_var).pack(side='left', padx=5)
        
        # Columns mapping frame
        columns_frame = ttk.LabelFrame(mapping_frame, text="Column Mapping")
        columns_frame.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Create two-pane view for columns
        columns_paned = ttk.PanedWindow(columns_frame, orient='horizontal')
        columns_paned.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Source columns list frame
        source_list_frame = ttk.Frame(columns_paned)
        ttk.Label(source_list_frame, text="Source Columns").pack()
        
        # Source columns
        self.source_columns = ttk.Treeview(source_list_frame, 
                                        columns=("name", "type"), 
                                        height=10, 
                                        show='headings')
        self.source_columns.heading("name", text="Column Name")
        self.source_columns.heading("type", text="Data Type")
        self.source_columns.column("name", width=150)
        self.source_columns.column("type", width=100)
        self.source_columns.pack(fill='both', expand=True)
        columns_paned.add(source_list_frame)
        
        # Mapping buttons frame
        button_frame = ttk.Frame(columns_paned)
        ttk.Button(button_frame, text="Map All >>", 
                command=self.map_all_columns).pack(pady=5)
        ttk.Button(button_frame, text="Map Selected >", 
                command=self.map_selected_columns).pack(pady=5)
        ttk.Button(button_frame, text="< Remove", 
                command=self.remove_mapped_columns).pack(pady=5)
        ttk.Button(button_frame, text="<< Remove All", 
                command=self.remove_all_mappings).pack(pady=5)
        columns_paned.add(button_frame)
        
        # Mapped columns frame
        mapped_frame = ttk.Frame(columns_paned)
        ttk.Label(mapped_frame, text="Mapped Columns").pack()
        
        # Mapped columns
        self.mapped_columns = ttk.Treeview(mapped_frame, 
                                        columns=("source", "target"), 
                                        height=10, 
                                        show='headings')
        self.mapped_columns.heading("source", text="Source Column")
        self.mapped_columns.heading("target", text="Target Property")
        self.mapped_columns.column("source", width=150)
        self.mapped_columns.column("target", width=150)
        self.mapped_columns.pack(fill='both', expand=True)
        columns_paned.add(mapped_frame)
        
        # Control buttons frame
        control_frame = ttk.Frame(frame)
        control_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(control_frame, text="Save Mapping", 
                command=self.save_current_mapping).pack(side='left', padx=5)
        ttk.Button(control_frame, text="Deploy Selected", 
                command=self.deploy_selected_mapping).pack(side='right', padx=5)
        ttk.Button(control_frame, text="Deploy All", 
                command=self.deploy_all_mappings).pack(side='right', padx=5)
        ttk.Button(control_frame, text="Show Status", 
                command=self.show_mapping_status).pack(side='right', padx=5)
        
        # Initialize mapping rules
        self.load_mapping_rules()
        
        return frame    

    def save_current_mapping(self):
        """Save current mapping configuration to file"""
        current = self.mapping_rule_var.get()
        if not current:
            messagebox.showwarning("Warning", "Please select or create a mapping rule first")
            return
            
        if not self.source_table_var.get() or not self.target_label_entry.get():
            messagebox.showwarning("Warning", "Please select source table and enter target label")
            return
            
        try:
            # Create mappings directory if doesn't exist
            if not os.path.exists('mappings'):
                os.makedirs('mappings')
                
            mapping = {
                'source': {
                    'type': self.source_var.get(),
                    'table': self.source_table_var.get()
                },
                'target': {
                    'type': 'node',
                    'label': self.target_label_entry.get()
                },
                'columns': {}
            }
            
            # Add column mappings
            for item in self.mapped_columns.get_children():
                values = self.mapped_columns.item(item)['values']
                mapping['columns'][values[0]] = values[1]
            
            # Save to file
            with open(f'mappings/{current}.json', 'w') as f:
                json.dump(mapping, f, indent=2)
                
            messagebox.showinfo("Success", f"Mapping rule '{current}' saved successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to save mapping rule: {str(e)}")
            messagebox.showerror("Error", "Failed to save mapping rule")

    def deploy_selected_mapping(self):
        """Deploy currently selected mapping rule to Kafka"""
        try:
            current = self.mapping_rule_var.get()
            if not current:
                messagebox.showwarning("Warning", "Please select a mapping rule to deploy")
                return False
                
            # Load mapping rule
            with open(f'mappings/{current}.json', 'r') as f:
                mapping = json.load(f)
            
            # Generate unique rule_id
            rule_id = str(uuid.uuid4())
            
            # Update monitoring database first
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            # Insert into deployed_rules
            c.execute('''INSERT OR REPLACE INTO deployed_rules 
                        (rule_id, rule_name, source_type, source_table, 
                        target_label, kafka_topic, status, last_updated, 
                        created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), 
                        datetime('now'), datetime('now'))''',
                    (rule_id, current, mapping['source']['type'],
                    mapping['source']['table'], mapping['target']['label'],
                    f"{self.kafka_entries['topic_prefix'].get()}_{current}",
                    'Deployed'))
            
            conn.commit()
            conn.close()

            # Create Kafka topics
            source_topic = f"{self.kafka_entries['topic_prefix'].get()}_{current}_source"
            sink_topic = f"{self.kafka_entries['topic_prefix'].get()}_{current}_sink"
            mapping_topic = f"{self.kafka_entries['topic_prefix'].get()}_mappings"
            
            # Create topics with proper configuration
            self.ensure_topics_exist([source_topic, sink_topic, mapping_topic])
            
            # Add topic information to mapping
            mapping['kafka'] = {
                'source_topic': source_topic,
                'sink_topic': sink_topic,
                'mapping_topic': mapping_topic
            }
            
            # Deploy mapping configuration
            producer = Producer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'acks': 'all'
            })
            
            producer.produce(
                mapping_topic,
                key=current.encode('utf-8'),
                value=json.dumps(mapping).encode('utf-8')
            )
            producer.flush(timeout=10)

            # Log the deployment
            self.log_kafka_action(
                action_type="DEPLOYMENT_SUCCESS",
                rule_name=current,
                topic_name=mapping_topic,
                status="SUCCESS",
                details=f"Successfully deployed mapping rule '{current}'"
            )

            # Refresh monitoring display
            self.refresh_monitoring()

            messagebox.showinfo("Success", 
                f"Mapping rule '{current}' deployed successfully\n\n"
                f"Topics created:\n"
                f"Source: {source_topic}\n"
                f"Sink: {sink_topic}\n"
                f"Mapping: {mapping_topic}")

            return True

        except Exception as e:
            self.logger.error(f"Deployment failed: {str(e)}")
            messagebox.showerror("Error", f"Failed to deploy mapping rule: {str(e)}")
            return False
        
    def verify_deployment(self, rule_name, topics):
        """Verify deployment by checking topics and configurations"""
        try:
            # Verify topics exist
            from confluent_kafka.admin import AdminClient
            admin_client = AdminClient({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get()
            })
            
            existing_topics = admin_client.list_topics().topics
            missing_topics = []
            
            for topic_type, topic_name in topics.items():
                if topic_name not in existing_topics:
                    missing_topics.append(f"{topic_type}: {topic_name}")
            
            if missing_topics:
                raise Exception(f"Missing topics:\n" + "\n".join(missing_topics))
                
            # Verify mapping in database
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            c.execute('''SELECT status FROM deployed_rules 
                        WHERE rule_name = ? AND status = 'Deployed' ''', 
                    (rule_name,))
            
            if not c.fetchone():
                raise Exception("Rule not properly registered in monitoring database")
                
            conn.close()
            
            # Verify mapping in Kafka
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_verify",
                'auto.offset.reset': 'earliest'
            })
            
            consumer.subscribe([topics['mapping_topic']])
            
            # Try to find the mapping configuration
            start_time = time.time()
            mapping_found = False
            
            while time.time() - start_time < 5:  # 5 second timeout
                msg = consumer.poll(timeout=1.0)
                if msg and not msg.error():
                    key = msg.key().decode('utf-8') if msg.key() else None
                    if key == rule_name:
                        mapping_found = True
                        break
                        
            consumer.close()
            
            if not mapping_found:
                raise Exception("Mapping configuration not found in Kafka")
                
            return True
            
        except Exception as e:
            self.logger.error(f"Deployment verification failed: {str(e)}")
            messagebox.showerror("Verification Error", 
                f"Deployment verification failed:\n{str(e)}")
            return False

    def monitor_topic_messages(self, topic_name):
        """Show messages in a specific topic"""
        try:
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_monitor",
                'auto.offset.reset': 'earliest'
            })
            
            consumer.subscribe([topic_name])
            
            messages_window = tk.Toplevel(self.root)
            messages_window.title(f"Messages in {topic_name}")
            messages_window.geometry("800x600")
            
            text = tk.Text(messages_window, wrap=tk.WORD)
            text.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Get last 10 messages
            messages = []
            while len(messages) < 10:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    break
                if not msg.error():
                    messages.append(msg)
                    
            for msg in messages:
                text.insert('end', f"Offset: {msg.offset()}\n")
                text.insert('end', f"Key: {msg.key().decode() if msg.key() else 'None'}\n")
                text.insert('end', f"Value: {msg.value().decode()}\n")
                text.insert('end', "-" * 80 + "\n\n")
                
            consumer.close()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to monitor topic: {str(e)}")
            
    def check_deployment_status(self, rule_name):
        """Check deployment status of a specific rule"""
        try:
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_verify",
                'auto.offset.reset': 'earliest'
            })
            
            topic = f"{self.kafka_entries['topic_prefix'].get()}_mappings"
            consumer.subscribe([topic])
            
            self.logger.info(f"Checking deployment status for rule: {rule_name}")
            found = False
            
            # Try to find the rule
            start_time = time.time()
            while time.time() - start_time < 5:  # 5 second timeout
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                
                if msg.error():
                    self.logger.error(f"Consumer error: {msg.error()}")
                    continue
                    
                try:
                    key = msg.key().decode('utf-8')
                    if key == rule_name:
                        found = True
                        value = json.loads(msg.value().decode('utf-8'))
                        self.logger.info(f"Found rule {rule_name} in topic:")
                        self.logger.info(f"  Offset: {msg.offset()}")
                        self.logger.info(f"  Partition: {msg.partition()}")
                        self.logger.info(f"  Timestamp: {msg.timestamp()}")
                        self.logger.info(f"  Configuration: {value}")
                        break
                except Exception as e:
                    self.logger.error(f"Error processing message: {str(e)}")
                    
            consumer.close()
            
            if not found:
                self.logger.warning(f"Rule {rule_name} not found in topic")
            
            return found
            
        except Exception as e:
            self.logger.error(f"Failed to check deployment status: {str(e)}")
            return False
    

    def deploy_all_mappings(self):
        """Deploy all mapping rules to Kafka"""
        try:
            mapping_files = glob.glob('mappings/*.json')
            if not mapping_files:
                messagebox.showinfo("Info", "No mapping rules found to deploy")
                return
            
            # Check Kafka connection first
            if not self.test_kafka_connection_silent():
                messagebox.showerror("Error", "Kafka is not connected. Please check Kafka configuration.")
                return

            producer = Producer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get()
            })
            
            topic = f"{self.kafka_entries['topic_prefix'].get()}_mappings"
            deployed_rules = []
            
            for mapping_file in mapping_files:
                rule_name = os.path.splitext(os.path.basename(mapping_file))[0]
                try:
                    with open(mapping_file, 'r') as f:
                        mapping = json.load(f)
                    
                    producer.produce(
                        topic,
                        key=rule_name.encode('utf-8'),
                        value=json.dumps(mapping).encode('utf-8')
                    )
                    deployed_rules.append(rule_name)
                    
                except Exception as e:
                    self.logger.error(f"Failed to deploy rule '{rule_name}': {str(e)}")
                    
            # Wait for all messages to be delivered
            producer.flush(timeout=10)
            
            # Verify deployments and update monitoring database
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            for rule_name in deployed_rules:
                # Update deployed_rules table
                c.execute('''INSERT OR REPLACE INTO deployed_rules 
                            (rule_name, source_type, source_table, target_label, status, last_updated)
                            VALUES (?, ?, ?, ?, ?, datetime('now'))''',
                        (rule_name, 
                        mapping['source']['type'],
                        mapping['source']['table'],
                        mapping['target']['label'],
                        'Deployed'))
                
                # Add initial status entry
                c.execute('''INSERT INTO integration_status 
                            (timestamp, rule_name, records_processed, success_count, error_count)
                            VALUES (datetime('now'), ?, 0, 0, 0)''',
                        (rule_name,))
            
            conn.commit()
            conn.close()

            # Refresh monitoring display
            self.refresh_monitoring()
            
            messagebox.showinfo("Success", f"Successfully deployed {len(deployed_rules)} mapping rules")
            
        except Exception as e:
            self.logger.error(f"Deployment failed: {str(e)}")
            messagebox.showerror("Error", f"Failed to deploy mapping rules: {str(e)}")


    def show_rule_status(self):
        """Show detailed status of selected rule"""
        selected = self.rules_tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Please select a rule to view status")
            return
            
        rule_name = self.rules_tree.item(selected[0])['values'][0]
        
        # Create status window
        status_window = tk.Toplevel(self.root)
        status_window.title(f"Rule Status: {rule_name}")
        status_window.geometry("500x400")
        
        # Status text
        text = tk.Text(status_window, wrap=tk.WORD, padx=10, pady=10)
        text.pack(fill='both', expand=True)
        
        try:
            # Load rule details
            with open(f'mappings/{rule_name}.json', 'r') as f:
                mapping = json.load(f)
                
            # Add rule information
            text.insert('end', f"Rule Name: {rule_name}\n\n")
            text.insert('end', f"Source:\n")
            text.insert('end', f"  Type: {mapping['source']['type']}\n")
            text.insert('end', f"  Table: {mapping['source']['table']}\n\n")
            text.insert('end', f"Target:\n")
            text.insert('end', f"  Label: {mapping['target']['label']}\n\n")
            text.insert('end', f"Mapped Columns: {len(mapping['columns'])}\n")
            text.insert('end', "  " + "\n  ".join(f"{src} -> {tgt}" 
                                                for src, tgt in mapping['columns'].items()))
            
            # Check deployment status
            deployed = self.check_rule_deployment(rule_name)
            text.insert('end', f"\n\nDeployment Status: {'Deployed' if deployed else 'Not Deployed'}\n")
            
        except Exception as e:
            text.insert('end', f"Error loading rule status: {str(e)}")
            
        text.configure(state='disabled')
        
    def show_mapping_status(self):
        """Show status of all mapping rules"""
        status_window = tk.Toplevel(self.root)
        status_window.title("Mapping Rules Status")
        status_window.geometry("600x400")
        
        # Create text widget for status display
        text = tk.Text(status_window, wrap=tk.WORD, padx=10, pady=10)
        text.pack(fill='both', expand=True)
        
        try:
            mapping_files = glob.glob('mappings/*.json')
            if not mapping_files:
                text.insert('end', "No mapping rules found\n")
                return
                
            for mapping_file in mapping_files:
                rule_name = os.path.splitext(os.path.basename(mapping_file))[0]
                
                try:
                    with open(mapping_file, 'r') as f:
                        mapping = json.load(f)
                        
                    text.insert('end', f"Rule: {rule_name}\n")
                    text.insert('end', f"Source: {mapping['source']['type']} - {mapping['source']['table']}\n")
                    text.insert('end', f"Target: {mapping['target']['label']}\n")
                    text.insert('end', f"Mapped Columns: {len(mapping['columns'])}\n")
                    text.insert('end', "-" * 40 + "\n\n")
                    
                except Exception as e:
                    text.insert('end', f"Error loading rule '{rule_name}': {str(e)}\n")
                    
        except Exception as e:
            text.insert('end', f"Error checking mapping rules: {str(e)}\n")
        
        text.configure(state='disabled')

        
    def load_mapping_rules(self):
        """Load existing mapping rules"""
        try:
            # Create mappings directory if it doesn't exist
            if not os.path.exists('mappings'):
                os.makedirs('mappings')
                
            # Load all mapping files
            mapping_files = glob.glob('mappings/*.json')
            rule_names = [os.path.splitext(os.path.basename(f))[0] for f in mapping_files]
            self.mapping_rule_combo['values'] = rule_names
            
        except Exception as e:
            self.logger.error(f"Failed to load mapping rules: {str(e)}")
            messagebox.showerror("Error", "Failed to load mapping rules")

    def clear_mapping_form(self):
        """Clear all mapping form fields"""
        self.source_table_var.set('')
        self.target_label_entry.delete(0, tk.END)
        self.mapped_columns.delete(*self.mapped_columns.get_children())
        self.source_columns.delete(*self.source_columns.get_children())

    def on_source_type_changed(self, *args):
        """Handle source type change"""
        # Reset connection state
        self.source_table_combo['state'] = 'disabled'
        self.source_table_combo['values'] = []
        self.source_table_var.set('')
        self.update_connection_status("source", False)
        
    def check_source_connection(self):
        """Check if source database is connected"""
        try:
            if self.source_var.get() == "postgresql":
                import psycopg2
                config = {key: entry.get() for key, entry in self.pg_entries.items()}
                conn = psycopg2.connect(**config)
                conn.close()
                return True
            elif self.source_var.get() == "mongodb":
                from pymongo import MongoClient
                config = {key: entry.get() for key, entry in self.mongo_entries.items()}
                client = MongoClient(f"mongodb://{config['host']}:{config['port']}/")
                client.server_info()  # Will raise exception if not connected
                client.close()
                return True
            return False
        except:
            return False
        
    def map_all_columns(self):
        """Map all source columns to target properties with same names"""
        try:
            # Clear existing mappings
            self.mapped_columns.delete(*self.mapped_columns.get_children())
            
            # Get all source columns
            for item in self.source_columns.get_children():
                column = self.source_columns.item(item)['values'][0]
                # Insert with same name for both source and target
                self.mapped_columns.insert('', 'end', values=(column, column))
                
            self.logger.info("Mapped all columns successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to map all columns: {str(e)}")
            messagebox.showerror("Error", "Failed to map all columns")

    def map_selected_columns(self):
        """Map selected source columns to target properties"""
        selected = self.source_columns.selection()
        
        for item in selected:
            column = self.source_columns.item(item)['values'][0]
            # Check if already mapped
            existing = False
            for mapped in self.mapped_columns.get_children():
                if self.mapped_columns.item(mapped)['values'][0] == column:
                    existing = True
                    break
            
            if not existing:
                self.mapped_columns.insert('', 'end', values=(column, column))

    def remove_mapped_columns(self):
        """Remove selected mapped columns"""
        selected = self.mapped_columns.selection()
        for item in selected:
            self.mapped_columns.delete(item)

    def remove_all_mappings(self):
        """Remove all mapped columns"""
        self.mapped_columns.delete(*self.mapped_columns.get_children())

    def save_mappings(self):
        """Save mapping rules to JSON file"""
        if not self.source_table_var.get() or not self.target_label_entry.get():
            messagebox.showwarning("Warning", "Please select source table and enter target label")
            return
            
        mappings = {
            'source': {
                'type': self.source_var.get(),
                'table': self.source_table_var.get()
            },
            'target': {
                'type': 'node',
                'label': self.target_label_entry.get()
            },
            'columns': {}
        }
        
        # Add column mappings
        for item in self.mapped_columns.get_children():
            values = self.mapped_columns.item(item)['values']
            mappings['columns'][values[0]] = values[1]
        
        try:
            with open('mapping_rules.json', 'w') as f:
                json.dump(mappings, f, indent=2)
            messagebox.showinfo("Success", "Mapping rules saved successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save mapping rules: {str(e)}")



    def check_kafka_deployment_readiness(self):
        """Comprehensive check of Kafka deployment prerequisites"""
        try:
            # Test basic connection
            bootstrap_servers = self.kafka_entries['bootstrap_servers'].get()
            producer = Producer({
                'bootstrap.servers': bootstrap_servers,
                'socket.timeout.ms': 5000
            })
            producer.flush(timeout=5)
            producer.close()

            # Test consumer connection
            consumer = Consumer({
                'bootstrap.servers': bootstrap_servers,
                'group.id': f"{self.kafka_entries['group_id'].get()}_test",
                'auto.offset.reset': 'earliest'
            })
            consumer.close()

            # Test topic creation permissions
            self.ensure_topic_exists(f"{self.kafka_entries['topic_prefix'].get()}_test")

            return {'ready': True, 'message': 'Kafka is ready for deployment'}

        except Exception as e:
            return {
                'ready': False,
                'message': f"Kafka is not fully ready: {str(e)}"
            }

    def ensure_topic_exists(self, topic_name):
        """Ensure the topic exists, create if it doesn't"""
        try:
            from confluent_kafka.admin import AdminClient, NewTopic
            
            admin_client = AdminClient({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get()
            })

            # Check if topic exists
            topics = admin_client.list_topics(timeout=5)
            if topic_name not in topics.topics:
                # Create topic
                new_topics = [NewTopic(
                    topic_name,
                    num_partitions=1,
                    replication_factor=1
                )]
                
                fs = admin_client.create_topics(new_topics)
                
                # Wait for topic creation
                for topic, f in fs.items():
                    try:
                        f.result(timeout=5)
                    except Exception as e:
                        self.logger.error(f"Failed to create topic {topic}: {str(e)}")
                        return False

            return True

        except Exception as e:
            self.logger.error(f"Failed to ensure topic exists: {str(e)}")
            return False


    def load_source_tables(self):
        """Load available tables/collections from source database"""
        try:
            tables = []
            if self.source_var.get() == "postgresql":
                import psycopg2
                config = {key: entry.get() for key, entry in self.pg_entries.items()}
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
                
            elif self.source_var.get() == "mongodb":
                from pymongo import MongoClient
                config = {key: entry.get() for key, entry in self.mongo_entries.items()}
                client = MongoClient(f"mongodb://{config['host']}:{config['port']}/")
                db = client[config['database']]
                tables = sorted(db.list_collection_names())
                client.close()

            return tables
                
        except Exception as e:
            self.logger.error(f"Failed to load source tables: {str(e)}")
            return []

    def on_rule_selected(self, event=None):
        """Handle rule selection in the main view"""
        selected = self.rules_tree.selection()
        if selected:
            rule_name = self.rules_tree.item(selected[0])['values'][0]
            self.show_rule_topics(rule_name)

    def show_rule_topics(self, rule_name):
        """Show topics for a specific rule"""
        if not rule_name:
            messagebox.showwarning("Warning", "Please select a mapping rule")
            return
            
        try:
            # Create topics window
            topics_window = tk.Toplevel(self.root)
            topics_window.title(f"Kafka Topics for {rule_name}")
            topics_window.geometry("800x500")
            
            # Create main frame to hold everything
            main_frame = ttk.Frame(topics_window)
            main_frame.pack(fill='both', expand=True, padx=10, pady=10)
            
            # Create Treeview frame at the top
            tree_frame = ttk.Frame(main_frame)
            tree_frame.pack(fill='both', expand=True)
            
            # Create Treeview
            topics_tree = ttk.Treeview(tree_frame, 
                columns=("type", "name", "partitions", "messages", "status"),
                show='headings')
                
            # Configure columns    
            topics_tree.heading("type", text="Topic Type")
            topics_tree.heading("name", text="Topic Name")
            topics_tree.heading("partitions", text="Partitions")
            topics_tree.heading("messages", text="Message Count")
            topics_tree.heading("status", text="Status")
            
            # Add scrollbar
            scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=topics_tree.yview)
            topics_tree.configure(yscrollcommand=scrollbar.set)
            
            # Pack tree and scrollbar
            topics_tree.pack(side='left', fill='both', expand=True)
            scrollbar.pack(side='right', fill='y')

            # Create buttons frame at the bottom
            button_frame = ttk.Frame(main_frame)
            button_frame.pack(fill='x', pady=(10, 0))

            def refresh_data():
                """Refresh the topic data"""
                # Clear existing items
                for item in topics_tree.get_children():
                    topics_tree.delete(item)
                    
                prefix = self.kafka_entries['topic_prefix'].get()
                topics = {
                    'Source': f"{prefix}_{rule_name}_source",
                    'Sink': f"{prefix}_{rule_name}_sink",
                    'Mapping': f"{prefix}_mappings"
                }
                
                try:
                    admin_client = AdminClient({
                        'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get()
                    })
                    
                    topic_metadata = admin_client.list_topics().topics
                    
                    for topic_type, topic_name in topics.items():
                        if topic_name in topic_metadata:
                            metadata = topic_metadata[topic_name]
                            
                            # Get message count
                            message_count = 0
                            for partition in metadata.partitions.values():
                                try:
                                    low, high = self.get_topic_offsets(topic_name, partition.id)
                                    message_count += high - low
                                except:
                                    message_count = "Error"
                            
                            topics_tree.insert('', 'end', values=(
                                topic_type,
                                topic_name,
                                len(metadata.partitions),
                                message_count,
                                "Active" if not metadata.error else "Error"
                            ))
                        else:
                            topics_tree.insert('', 'end', values=(
                                topic_type,
                                topic_name,
                                "N/A",
                                "N/A",
                                "Not Found"
                            ))
                except Exception as e:
                    self.logger.error(f"Failed to get Kafka metadata: {str(e)}")
                    for topic_type, topic_name in topics.items():
                        topics_tree.insert('', 'end', values=(
                            topic_type,
                            topic_name,
                            "Error",
                            "Error",
                            "Error"
                        ))

            # Add to the refresh_data function in show_rule_topics:
            sync_status = self.get_sync_status(rule_name)
            if sync_status:
                ttk.Label(status_frame, text=f"Last Sync: {sync_status['last_sync']}\n"
                                            f"Status: {sync_status['status']}").pack()
                
            # Add buttons at the bottom
            ttk.Button(button_frame, text="Refresh", 
                    command=refresh_data).pack(side='left', padx=5)
            ttk.Button(button_frame, text="View Messages",
                    command=lambda: self.show_topic_messages(
                        topics_tree.item(topics_tree.selection()[0])['values'][1]
                        if topics_tree.selection() else None
                    )).pack(side='left', padx=5)
            ttk.Button(button_frame, text="Start Sync",
                    command=lambda: self.start_sync_for_rule(rule_name)).pack(side='left', padx=5)
            
            # Add start/stop buttons
            ttk.Button(button_frame, text="Start Processing",
                    command=lambda: self.start_background_threads(rule_name)).pack(side='left', padx=5)
            ttk.Button(button_frame, text="Stop Processing",
                    command=lambda: self.stop_background_threads(rule_name)).pack(side='left', padx=5)
            


            # Initial data load
            refresh_data()
            
        except Exception as e:
            self.logger.error(f"Failed to show topics: {str(e)}")
            messagebox.showerror("Error", f"Failed to show topic details: {str(e)}")

    def check_thread_status(self, rule_name):
        """Check if threads are running for a rule"""
        source_running = (rule_name in self.source_threads and 
                        self.source_threads[rule_name] is not None and 
                        self.source_threads[rule_name].is_alive())
        
        sink_running = (rule_name in self.sink_threads and 
                    self.sink_threads[rule_name] is not None and 
                    self.sink_threads[rule_name].is_alive())
        
        return source_running, sink_running

                
    def get_topic_offsets(self, topic, partition):
        """Get low and high watermarks for a topic partition"""
        try:
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_offset_check"
            })
            
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition),
                timeout=5.0
            )
            
            consumer.close()
            return low, high
            
        except Exception as e:
            self.logger.error(f"Failed to get offsets: {str(e)}")
            return 0, 0

    def monitor_sync_progress(self, rule_name):
        """Monitor synchronization progress"""
        try:
            prefix = self.kafka_entries['topic_prefix'].get()
            source_topic = f"{prefix}_{rule_name}_source"
            sink_topic = f"{prefix}_{rule_name}_sink"
            
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_monitor"
            })
            
            # Get message counts
            source_count = 0
            sink_count = 0
            
            for topic in [source_topic, sink_topic]:
                partitions = consumer.list_topics(topic).topics[topic].partitions
                for partition in partitions:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(topic, partition),
                        timeout=5.0
                    )
                    if topic == source_topic:
                        source_count += high - low
                    else:
                        sink_count += high - low
                        
            consumer.close()
            
            return {
                'source_messages': source_count,
                'processed_messages': sink_count,
                'pending_messages': source_count - sink_count
            }
            
        except Exception as e:
            self.logger.error(f"Failed to monitor sync progress: {str(e)}")
            return None
            
    def refresh_topic_view(self, window, tree, rule_name):
        """Refresh the topic view"""
        window.destroy()
        self.show_rule_topics(rule_name)

    def view_topic_messages(self, tree):
        """View messages in selected topic"""
        selected = tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Please select a topic to view messages")
            return
            
        topic_name = tree.item(selected[0])['values'][1]  # Get topic name from selected row
        self.show_topic_messages(topic_name)

    def create_neo4j_label(self, label, properties):
        """Create a new label in Neo4j with specified properties"""
        try:
            from neo4j import GraphDatabase
            config = {key: entry.get() for key, entry in self.neo4j_entries.items()}
            
            driver = GraphDatabase.driver(
                config['url'],
                auth=(config['user'], config['password'])
            )

            with driver.session() as session:
                # First create the label if it doesn't exist
                session.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE")
                
                # Create initial node with properties
                property_string = ", ".join([f"n.{key} = ${key}" for key in properties])
                query = f"""
                    MERGE (n:{label} {{id: $id}})
                    SET {property_string}
                    RETURN n
                """
                session.run(query, **properties)

            driver.close()
            self.logger.info(f"Successfully created/updated label {label}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create Neo4j label: {str(e)}")
            return False
        


    def initialize_neo4j_schema(self, label):
        """Initialize Neo4j schema for a label"""
        try:
            from neo4j import GraphDatabase
            config = {key: entry.get() for key, entry in self.neo4j_entries.items()}
            
            driver = GraphDatabase.driver(
                config['url'],
                auth=(config['user'], config['password'])
            )

            with driver.session() as session:
                # Create constraint for unique IDs
                session.run(f"""
                    CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label})
                    REQUIRE n.id IS UNIQUE
                """)
                
                # Create initial empty node to ensure label exists
                session.run(f"""
                    MERGE (n:{label} {{id: 'init'}})
                    RETURN n
                """)

            driver.close()
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize Neo4j schema: {str(e)}")
            return False
                   
    def update_sync_status(self, rule_name, processed, success, errors):
        try:
            with sqlite3.connect('monitoring.db') as conn:
                c = conn.cursor()
                c.execute('''INSERT INTO integration_status 
                            (timestamp, rule_name, records_processed, 
                            success_count, error_count, status)
                            VALUES (datetime('now'), ?, ?, ?, ?, ?)''',
                        (rule_name, processed, success, errors, 
                        'ERROR' if errors > 0 else 'SUCCESS'))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Status update failed: {str(e)}")


    def validate_kafka_message(self, message, mapping):
        try:
            data = json.loads(message.value().decode('utf-8'))
            required_fields = mapping['columns'].keys()
            missing_fields = [f for f in required_fields if f not in data]
            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")
            return True, data
        except Exception as e:
            return False, str(e)

    def start_sync_for_rule(self, rule_name):
        """Start synchronization for a specific rule"""
        try:
            # Log sync start
            self.log_kafka_action(
                action_type="SYNC_START",
                rule_name=rule_name,
                details="Starting data synchronization"
            )
            
            self.track_sync_operation(
                rule_name=rule_name,
                status="STARTED",
                details="Initializing synchronization"
            )

            # Load mapping configuration
            with open(f'mappings/{rule_name}.json', 'r') as f:
                mapping = json.load(f)
                
            # Initialize Neo4j schema first
            if not self.initialize_neo4j_schema(mapping['target']['label']):
                raise Exception("Failed to initialize Neo4j schema")

            # Set up topics
            source_topic = f"{self.kafka_entries['topic_prefix'].get()}_{rule_name}_source"
            sink_topic = f"{self.kafka_entries['topic_prefix'].get()}_{rule_name}_sink"

            # Create topics if needed
            self.ensure_topics_exist([source_topic, sink_topic])

            # Start consumer thread first
            if not self.start_sync_consumer(rule_name):
                raise Exception("Failed to start sync consumer")

            # Create source data producer
            producer = Producer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'client.id': f'sync_producer_{rule_name}',
                'acks': 'all',
                'retries': 3,
                'delivery.timeout.ms': 10000
            })
            
            # Get source data
            if mapping['source']['type'] == 'postgresql':
                import psycopg2
                from psycopg2.extras import RealDictCursor  # Add this import
                
                config = {key: entry.get() for key, entry in self.pg_entries.items()}
                conn = psycopg2.connect(**config)
                cursor = conn.cursor(cursor_factory=RealDictCursor)  # Use RealDictCursor
                
                # Get column names
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{mapping['source']['table']}'
                    ORDER BY ordinal_position
                """)
                columns = [row['column_name'] for row in cursor.fetchall()]
                
                # Get all data
                cursor.execute(f"SELECT * FROM {mapping['source']['table']}")
                
                batch_size = 1000
                total_count = 0
                batch_number = 0
                
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                        
                    for row in rows:
                        # Convert row to dictionary and handle special types
                        message = {}
                        for column, value in row.items():
                            if value is not None:
                                if isinstance(value, (datetime, date)):
                                    value = value.isoformat()
                                elif isinstance(value, (Decimal, UUID)):
                                    value = str(value)
                                elif isinstance(value, bytes):
                                    value = value.hex()
                            message[column] = value

                        try:
                            # Use first column as key, or generate UUID
                            key = str(row[columns[0]]) if row[columns[0]] is not None else str(uuid.uuid4())
                            producer.produce(
                                source_topic,
                                key=key.encode('utf-8'),
                                value=json.dumps(message).encode('utf-8'),
                                on_delivery=lambda err, msg: self.delivery_callback(err, msg, rule_name)
                            )
                            total_count += 1

                            # Flush periodically
                            if total_count % 100 == 0:
                                producer.flush()

                        except BufferError:
                            producer.flush()
                            # Retry the failed message
                            producer.produce(
                                source_topic,
                                key=key.encode('utf-8'),
                                value=json.dumps(message).encode('utf-8'),
                                on_delivery=lambda err, msg: self.delivery_callback(err, msg, rule_name)
                            )

                    batch_number += 1
                    self.logger.info(f"Processed batch {batch_number} ({total_count} messages so far)")
                    producer.flush()

                conn.close()
                producer.flush()  # Final flush

                # Log successful sync start
                self.log_kafka_action(
                    action_type="SYNC_RUNNING",
                    rule_name=rule_name,
                    details=f"Sync process started successfully"
                )

                messagebox.showinfo("Success", 
                    f"Started sync for rule '{rule_name}'\n"
                    f"Produced {total_count} messages to topic {source_topic}")
                return True
            
        except Exception as e:
            # Log sync failure
            self.log_kafka_action(
                action_type="SYNC_FAILURE",
                rule_name=rule_name,
                status="ERROR",
                error_message=str(e),
                details="Failed to start synchronization"
            )
            
            self.track_sync_operation(
                rule_name=rule_name,
                status="FAILED",
                details=f"Sync failed: {str(e)}"
            )

            self.logger.error(f"Failed to start sync: {str(e)}")
            messagebox.showerror("Error", f"Failed to start sync: {str(e)}")
            return False
        
    def show_action_history(self):
        """Display Kafka action history"""
        try:
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            # Get recent actions
            c.execute('''SELECT action_type, rule_name, topic_name, status, 
                        timestamp, details, error_message 
                        FROM kafka_actions 
                        ORDER BY timestamp DESC LIMIT 100''')
            
            actions = c.fetchall()
            conn.close()
            
            # Create history window
            history_window = tk.Toplevel(self.root)
            history_window.title("Kafka Action History")
            history_window.geometry("800x600")
            
            # Create Treeview
            tree = ttk.Treeview(history_window, 
                            columns=("timestamp", "action", "rule", "status", "details"),
                            show='headings')
            
            tree.heading("timestamp", text="Timestamp")
            tree.heading("action", text="Action")
            tree.heading("rule", text="Rule Name")
            tree.heading("status", text="Status")
            tree.heading("details", text="Details")
            
            # Add actions to tree
            for action in actions:
                tree.insert('', 'end', values=(
                    action[4],  # timestamp
                    action[0],  # action_type
                    action[1],  # rule_name
                    action[3],  # status
                    action[5]   # details
                ))
            
            tree.pack(fill='both', expand=True)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to show action history: {str(e)}")
            
    def ensure_topics_exist(self, topic_list):
        """Ensure all required topics exist"""
        try:
            from confluent_kafka.admin import AdminClient, NewTopic
            
            admin_client = AdminClient({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get()
            })

            # Get existing topics
            existing_topics = admin_client.list_topics().topics

            # Configure new topics
            topic_configs = {
                'cleanup.policy': 'compact',
                'retention.ms': '604800000',  # 7 days
                'delete.retention.ms': '86400000'  # 1 day
            }

            # Create list of topics that need to be created
            new_topics = []
            for topic in topic_list:
                if topic not in existing_topics:
                    new_topics.append(NewTopic(
                        topic,
                        num_partitions=1,
                        replication_factor=1,
                        config=topic_configs
                    ))

            # Create new topics if needed
            if new_topics:
                fs = admin_client.create_topics(new_topics)
                for topic, f in fs.items():
                    try:
                        f.result(timeout=5)
                        self.logger.info(f"Created topic: {topic}")
                    except Exception as e:
                        self.logger.error(f"Failed to create topic {topic}: {str(e)}")
                        raise

            return True

        except Exception as e:
            self.logger.error(f"Failed to ensure topics exist: {str(e)}")
            raise        
        
    def start_sync_consumer(self, rule_name):
        """Start consuming messages and sinking to Neo4j"""
        try:
            # Load mapping configuration
            with open(f'mappings/{rule_name}.json', 'r') as f:
                mapping = json.load(f)

            source_topic = f"{self.kafka_entries['topic_prefix'].get()}_{rule_name}_source"
            sink_topic = f"{self.kafka_entries['topic_prefix'].get()}_{rule_name}_sink"

            # Configure consumer with proper settings
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_sink_{rule_name}",
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
                'max.poll.interval.ms': 300000,  # 5 minutes
                'session.timeout.ms': 45000,
                'heartbeat.interval.ms': 15000
            })

            consumer.subscribe([source_topic])

            # Configure producer
            producer = Producer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'client.id': f'sink_producer_{rule_name}',
                'acks': 'all',
                'retries': 3
            })

            def sync_process():
                try:
                    while True:
                        msg = consumer.poll(timeout=1.0)
                        if msg is None:
                            continue

                        if msg.error():
                            self.logger.error(f"Consumer error: {msg.error()}")
                            continue

                        try:
                            # Validate message format
                            is_valid, data = self.validate_kafka_message(msg, mapping)
                            if not is_valid:
                                self.logger.error(f"Invalid message format: {data}")
                                self.update_sync_status(rule_name, 1, 0, 1)
                                consumer.commit(msg)
                                continue

                            # Process message
                            if self.process_sink_message(msg, mapping):
                                producer.produce(
                                    sink_topic,
                                    key=msg.key(),
                                    value=msg.value(),
                                    on_delivery=lambda err, msg: self.delivery_callback(err, msg, rule_name)
                                )
                                producer.flush()
                                consumer.commit(msg)
                                self.update_sync_status(rule_name, 1, 1, 0)
                            else:
                                self.update_sync_status(rule_name, 1, 0, 1)

                        except Exception as e:
                            self.logger.error(f"Error processing message: {str(e)}")
                            self.update_sync_status(rule_name, 1, 0, 1)

                except Exception as e:
                    self.logger.error(f"Sync process error: {str(e)}")
                finally:
                    consumer.close()
                    producer.flush()

            # Start sync thread
            sync_thread = threading.Thread(target=sync_process, daemon=True)
            sync_thread.start()
            
            # Store thread reference
            self.sync_threads = getattr(self, 'sync_threads', {})
            self.sync_threads[rule_name] = sync_thread

            return True

        except Exception as e:
            self.logger.error(f"Failed to start sync consumer: {str(e)}")
            return False

    def process_sink_message(self, message, mapping):
        """Process a message and sink it to Neo4j"""
        try:
            # Parse message
            data = json.loads(message.value().decode('utf-8'))
            label = mapping['target']['label']
            
            # Prepare properties with proper type conversion
            properties = {}
            for source_col, target_prop in mapping['columns'].items():
                if source_col in data:
                    value = data[source_col]
                    if isinstance(value, (int, float, str, bool)) and value is not None:
                        properties[target_prop] = value
                    elif isinstance(value, (dict, list)):
                        properties[target_prop] = json.dumps(value)
                    elif value is not None:
                        properties[target_prop] = str(value)

            # Ensure ID property
            if 'id' not in properties:
                properties['id'] = str(uuid.uuid4())

            # Connect to Neo4j
            from neo4j import GraphDatabase
            config = {key: entry.get() for key, entry in self.neo4j_entries.items()}
            
            driver = GraphDatabase.driver(
                config['url'],
                auth=(config['user'], config['password'])
            )

            with driver.session() as session:
                try:
                    # Create merge query
                    property_string = ", ".join([f"n.{key} = ${key}" for key in properties])
                    query = f"""
                        MERGE (n:{label} {{id: $id}})
                        SET {property_string}
                        RETURN n
                    """
                    session.run(query, **properties)
                    
                except Exception as e:
                    if "UnknownLabelWarning" in str(e):
                        # Create label and retry
                        self.initialize_neo4j_schema(label)
                        session.run(query, **properties)
                    else:
                        raise e

            driver.close()
            return True

        except Exception as e:
            self.logger.error(f"Failed to process sink message: {str(e)}")
            return False
    
    
    def stop_sync_for_rule(self, rule_name):
        """Stop synchronization for a specific rule"""
        try:
            if hasattr(self, 'sync_threads') and rule_name in self.sync_threads:
                # Thread will terminate when main loop exits
                self.sync_threads[rule_name] = None
                self.logger.info(f"Stopped sync for rule {rule_name}")
        except Exception as e:
            self.logger.error(f"Failed to stop sync: {str(e)}")

if __name__ == "__main__":
    app = DataIntegrationIDE()
    app.root.mainloop()
            
