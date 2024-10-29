import tkinter as tk
from tkinter import ttk, messagebox

import configparser
import json
import sqlite3
import logging
import socket
import requests
# Kafka imports
try:
    from confluent_kafka import Producer, Consumer, KafkaException
except ImportError:
    messagebox.showerror("Import Error", 
        "confluent-kafka is not installed.\n"
        "Please install it using: pip install confluent-kafka")

class DataIntegrationIDE:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Data Integration IDE")
        self.current_step = 0
        self.config = configparser.ConfigParser()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        self.load_config()
        self.setup_ui()

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
        color = "#2ECC71" if is_connected else "#95A5A6"
        
        if connection_type == "source":
            self.source_indicator.delete("all")
            self.source_indicator.create_oval(2, 2, 10, 10, fill=color, outline=color)
            if db_type:
                self.source_type_label.config(text=f"{db_type}")
            else:
                self.source_type_label.config(text="Disconnected")
        elif connection_type == "target":
            self.target_indicator.delete("all")
            self.target_indicator.create_oval(2, 2, 10, 10, fill=color, outline=color)
            if db_type:
                self.target_type_label.config(text=f"{db_type}")
            else:
                self.target_type_label.config(text="Disconnected")
        elif connection_type == "kafka":
            self.kafka_indicator.delete("all")
            self.kafka_indicator.create_oval(2, 2, 10, 10, fill=color, outline=color)
            if db_type:
                self.kafka_type_label.config(text=f"{db_type}")
            else:
                self.kafka_type_label.config(text="Disconnected")


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


    def create_steps(self):
        """Create notebook tabs for each integration step"""
        # Create frames for each step
        self.frames = {}
        # In create_steps method
        self.frames["Source Selection"] = self.create_source_selection()
        self.frames["Target Config"] = self.create_target_config()
        self.frames["Kafka Config"] = self.create_kafka_config()  # Add this line
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
        
        return frame

    def save_kafka_config(self):
        """Save Kafka configuration to db.ini"""
        try:
            if 'kafka' not in self.config:
                self.config['kafka'] = {}
                
            for key, entry in self.kafka_entries.items():
                self.config['kafka'][key] = entry.get()
            
            self.config['kafka']['auto_offset_reset'] = self.auto_offset.get()
            
            self.save_config()
            messagebox.showinfo("Success", "Kafka configuration saved successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save Kafka configuration: {str(e)}")


    def check_kafka_service_status(self):
        """Check if Kafka service is running and accessible"""
        try:
            from confluent_kafka import Producer, KafkaException
            import socket
            import requests
            
            status = {
                'zookeeper': False,
                'kafka': False,
                'schema_registry': False,
                'connect': False
            }
            
            # Check Zookeeper
            try:
                zookeeper = self.kafka_entries['zookeeper'].get()
                zk_host, zk_port = zookeeper.split(':')
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                result = sock.connect_ex((zk_host, int(zk_port)))
                sock.close()
                status['zookeeper'] = result == 0
            except Exception as e:
                self.logger.error(f"Zookeeper check failed: {str(e)}")
            
            # Check Kafka
            try:
                bootstrap_servers = self.kafka_entries['bootstrap_servers'].get()
                producer = Producer({
                    'bootstrap.servers': bootstrap_servers,
                    'socket.timeout.ms': 5000
                })
                producer.flush(timeout=5)
                status['kafka'] = True
            except Exception as e:
                self.logger.error(f"Kafka check failed: {str(e)}")
            
            # Check Schema Registry
            try:
                schema_registry_url = self.kafka_entries['schema_registry'].get()
                response = requests.get(f"{schema_registry_url}/subjects", timeout=5)
                status['schema_registry'] = response.status_code == 200
            except Exception as e:
                self.logger.error(f"Schema Registry check failed: {str(e)}")
            
            # Check Kafka Connect
            try:
                connect_url = self.kafka_entries['connect_rest'].get()
                response = requests.get(f"{connect_url}/connectors", timeout=5)
                status['connect'] = response.status_code == 200
            except Exception as e:
                self.logger.error(f"Kafka Connect check failed: {str(e)}")
            
            return status
            
        except Exception as e:
            self.logger.error(f"Error checking Kafka service status: {str(e)}")
            return {
                'zookeeper': False,
                'kafka': False,
                'schema_registry': False,
                'connect': False
            }

    def show_kafka_status(self):
        """Display Kafka service status"""
        status = self.check_kafka_service_status()
        
        status_frame = ttk.Frame()
        status_window = tk.Toplevel(self.root)
        status_window.title("Kafka Services Status")
        status_window.geometry("400x300")
        
        # Status indicators
        services = {
            'Zookeeper': status['zookeeper'],
            'Kafka Broker': status['kafka'],
            'Schema Registry': status['schema_registry'],
            'Kafka Connect': status['connect']
        }
        
        for i, (service, is_running) in enumerate(services.items()):
            frame = ttk.Frame(status_window)
            frame.pack(fill='x', padx=20, pady=5)
            
            ttk.Label(frame, text=f"{service}:").pack(side='left')
            
            status_canvas = tk.Canvas(frame, width=12, height=12)
            status_canvas.pack(side='right')
            
            color = "#2ECC71" if is_running else "#E74C3C"
            status_canvas.create_oval(2, 2, 10, 10, fill=color, outline=color)
            
            status_label = ttk.Label(frame, 
                text="Running" if is_running else "Not Running")
            status_label.pack(side='right', padx=5)
        
        # Add refresh button
        ttk.Button(status_window, text="Refresh Status", 
                command=lambda: self.refresh_status(status_window)).pack(pady=10)
        

    def refresh_status(self, status_window):
        """Refresh the status window"""
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
            
            producer.close()
            
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
            
            # Additional message test
            if self.test_kafka_message():
                self.update_connection_status("kafka", True, "Connected")
                messagebox.showinfo("Success", "Kafka connection successful!")
            else:
                raise ConnectionError("Failed to verify message passing")
            
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
            self.update_connection_status("kafka", False)
            self.logger.error(f"Kafka connection error: {str(e)}")
            messagebox.showerror("Error", 
                f"Failed to connect to Kafka:\n{str(e)}")
            
            
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


    def create_mapping_rules(self):
        frame = ttk.Frame(self.notebook)
        
        # Rule definition
        rule_frame = ttk.LabelFrame(frame, text="Mapping Rules")
        rule_frame.pack(pady=10, padx=10, fill="both", expand=True)
        
        # Source fields
        self.source_fields = ttk.Treeview(rule_frame, columns=("field", "type"))
        self.source_fields.heading("field", text="Source Field")
        self.source_fields.heading("type", text="Data Type")
        
        # Target nodes/relationships
        self.target_elements = ttk.Treeview(rule_frame, 
                                        columns=("element", "type"))
        self.target_elements.heading("element", text="Neo4j Element")
        self.target_elements.heading("type", text="Type")
        
        return frame
        
    def create_monitoring(self):
        frame = ttk.Frame(self.notebook)
        
        # SQLite monitoring setup
        monitor_frame = ttk.LabelFrame(frame, text="Monitoring Dashboard")
        monitor_frame.pack(pady=10, padx=10, fill="both", expand=True)
        
        # Status display
        self.status_tree = ttk.Treeview(monitor_frame, 
                                    columns=("timestamp", "status", "records"))
        self.status_tree.heading("timestamp", text="Timestamp")
        self.status_tree.heading("status", text="Status")
        self.status_tree.heading("records", text="Records Processed")
        
        return frame


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

    def init_monitoring_db(self):
        conn = sqlite3.connect('monitoring.db')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS integration_status
                    (timestamp TEXT, status TEXT, records_processed INTEGER)''')
        conn.commit()
        conn.close()

if __name__ == "__main__":
    app = DataIntegrationIDE()
    app.root.mainloop()
            