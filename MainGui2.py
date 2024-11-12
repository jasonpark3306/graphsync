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

# Kafka imports
try:
    from confluent_kafka import Producer, Consumer, KafkaException
except ImportError:
    messagebox.showerror("Import Error", 
        "confluent-kafka is not installed.\n"
        "Please install it using: pip install confluent-kafka")

class DataIntegrationIDE:
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
        
        # Initialize database
        self.init_monitoring_db()  # Add this line
        
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
                self.kafka_type_label.config(
                    text=f"{db_type}",
                    foreground=color
                )
            else:
                self.kafka_type_label.config(
                    text="Disconnected",
                    foreground=color
                )

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

            except Exception as e:
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

    def show_topic_messages(self, topic):
        """Show recent messages in topic"""
        try:
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_view",
                'auto.offset.reset': 'earliest'
            })
            
            consumer.subscribe([topic])
            
            # Create message viewer window
            viewer = tk.Toplevel(self.root)
            viewer.title(f"Messages in {topic}")
            viewer.geometry("600x400")
            
            # Message display
            text_widget = tk.Text(viewer, wrap=tk.WORD, padx=10, pady=10)
            text_widget.pack(fill='both', expand=True)
            
            # Get messages (last 10)
            messages = []
            while len(messages) < 10:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    break
                if not msg.error():
                    messages.append({
                        'timestamp': msg.timestamp()[1],
                        'key': msg.key().decode('utf-8') if msg.key() else None,
                        'value': json.loads(msg.value().decode('utf-8'))
                    })
            
            if messages:
                for msg in messages:
                    text_widget.insert('end', 
                        f"Timestamp: {msg['timestamp']}\n"
                        f"Key: {msg['key']}\n"
                        f"Value: {json.dumps(msg['value'], indent=2)}\n"
                        f"{'-'*60}\n\n"
                    )
            else:
                text_widget.insert('end', "No messages found in topic")
            
            consumer.close()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read topic messages: {str(e)}")

    def test_complete_workflow(self):
        """Test complete workflow from mapping to monitoring"""
        try:
            # 1. Verify source connection
            if not self.check_source_connection():
                return "Source database not connected"
                
            # 2. Verify Kafka connection
            if not self.test_kafka_connection_silent():
                return "Kafka not connected"
                
            # 3. Verify Neo4j connection
            try:
                self.test_neo4j_connection()
            except:
                return "Neo4j not connected"
                
            # 4. Check mapping deployment
            topic = f"{self.kafka_entries['topic_prefix'].get()}_mappings"
            producer = Producer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get()
            })
            producer.flush()
            
            # 5. Verify monitoring setup
            if not self.check_database_status():
                return "Monitoring database not initialized"
                
            return "All systems verified and working"
            
        except Exception as e:
            return f"Workflow test failed: {str(e)}"
                
    def create_monitoring(self):
        frame = ttk.Frame(self.notebook)
        
        # Create main layout with three panes instead of two
        paned = ttk.PanedWindow(frame, orient='vertical')
        paned.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Top frame for deployed rules
        rules_frame = ttk.LabelFrame(paned, text="Deployed Mapping Rules")
        paned.add(rules_frame)
        
        # Rules Treeview - Add topic column
        self.rules_tree = ttk.Treeview(rules_frame, 
                                    columns=("rule", "source", "target", "topic", "status"),
                                    show='headings',
                                    height=6)
        self.rules_tree.heading("rule", text="Rule Name")
        self.rules_tree.heading("source", text="Source")
        self.rules_tree.heading("target", text="Target")
        self.rules_tree.heading("topic", text="Kafka Topic")  # New column
        self.rules_tree.heading("status", text="Status")
        
        self.rules_tree.column("rule", width=150)
        self.rules_tree.column("source", width=150)
        self.rules_tree.column("target", width=150)
        self.rules_tree.column("topic", width=150)  # New column
        self.rules_tree.column("status", width=100)
        
        # Add scrollbar to rules tree
        rules_scroll = ttk.Scrollbar(rules_frame, orient="vertical", command=self.rules_tree.yview)
        self.rules_tree.configure(yscrollcommand=rules_scroll.set)
        
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
        
        self.status_tree.column("timestamp", width=150)
        self.status_tree.column("rule", width=150)
        self.status_tree.column("records", width=100)
        self.status_tree.column("success", width=100)
        self.status_tree.column("errors", width=100)
        
        # Add scrollbar to status tree
        status_scroll = ttk.Scrollbar(status_frame, orient="vertical", command=self.status_tree.yview)
        self.status_tree.configure(yscrollcommand=status_scroll.set)
        
        self.status_tree.pack(side='left', fill='both', expand=True)
        status_scroll.pack(side='right', fill='y')

        # Control frame
        control_frame = ttk.Frame(frame)
        control_frame.pack(fill='x', padx=10, pady=5)

        # Add to control_frame in create_monitoring
        ttk.Button(control_frame, text="Diagnostic Check", 
                command=lambda: messagebox.showinfo(
                    "System Check", 
                    self.test_complete_workflow()
                )).pack(side='left', padx=5)
        
        # Left side buttons
        left_buttons_frame = ttk.Frame(control_frame)
        left_buttons_frame.pack(side='left')
        
        ttk.Button(left_buttons_frame, text="Refresh Status", 
                command=self.refresh_monitoring).pack(side='left', padx=5)
        ttk.Button(left_buttons_frame, text="View Details", 
                command=self.show_sync_details).pack(side='left', padx=5)
        
        # Right side buttons
        right_buttons_frame = ttk.Frame(control_frame)
        right_buttons_frame.pack(side='right')
        
        ttk.Button(right_buttons_frame, text="Verify Sync", 
                command=self.verify_data_sync).pack(side='left', padx=5)
        ttk.Button(right_buttons_frame, text="Start Monitoring", 
                command=self.start_monitoring).pack(side='left', padx=5)
        ttk.Button(right_buttons_frame, text="Stop Monitoring", 
                command=self.stop_monitoring).pack(side='left', padx=5)
             
        # Additional buttons in rules_actions frame
        ttk.Button(rules_actions, text="View Details", 
                command=self.show_sync_details).pack(side='left', padx=5)
        
        ttk.Button(rules_actions, text="Refresh Rules", 
                command=self.refresh_monitoring).pack(side='left', padx=5)
        
    
        # Status bar
        status_bar = ttk.Frame(frame)
        status_bar.pack(fill='x', padx=10, pady=5)
        
        # Connection status indicators frame
        conn_status_frame = ttk.Frame(status_bar)
        conn_status_frame.pack(side='left')
        
        # Source status with icon
        source_frame = ttk.Frame(conn_status_frame)
        source_frame.pack(side='left', padx=10)
        self.source_status = ttk.Label(source_frame, text="Source: Not Connected")
        self.source_status.pack(side='left')
        self.source_indicator = tk.Canvas(source_frame, width=12, height=12)
        self.source_indicator.pack(side='left', padx=5)
        
        # Sink status with icon
        sink_frame = ttk.Frame(conn_status_frame)
        sink_frame.pack(side='left', padx=10)
        self.sink_status = ttk.Label(sink_frame, text="Sink: Not Connected")
        self.sink_status.pack(side='left')
        self.sink_indicator = tk.Canvas(sink_frame, width=12, height=12)
        self.sink_indicator.pack(side='left', padx=5)
        
        # Initialize monitoring state
        self.monitoring_active = False
        self.update_monitoring_status()
        
        # Update initial status indicators
        self.update_connection_indicators()
        
        return frame

    def update_connection_indicators(self):
        """Update the connection status indicators"""
        def draw_indicator(canvas, connected):
            canvas.delete("all")
            color = "#2ECC71" if connected else "#E74C3C"  # Green if connected, red if not
            canvas.create_oval(2, 2, 10, 10, fill=color, outline=color)
        
        # Update source indicator
        draw_indicator(self.source_indicator, self.monitoring_active)
        self.source_status.config(
            text="Source: Connected" if self.monitoring_active else "Source: Not Connected",
            foreground="#2ECC71" if self.monitoring_active else "#E74C3C"
        )
        
        # Update sink indicator
        draw_indicator(self.sink_indicator, self.monitoring_active)
        self.sink_status.config(
            text="Sink: Connected" if self.monitoring_active else "Sink: Not Connected",
            foreground="#2ECC71" if self.monitoring_active else "#E74C3C"
        )


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
        
        self.rules_tree.column("rule", width=150)
        self.rules_tree.column("source", width=150)
        self.rules_tree.column("target", width=150)
        self.rules_tree.column("topic", width=150)
        self.rules_tree.column("status", width=100)
        
        # Add scrollbar to rules tree
        rules_scroll = ttk.Scrollbar(rules_frame, orient="vertical", command=self.rules_tree.yview)
        self.rules_tree.configure(yscrollcommand=rules_scroll.set)
        
        # Pack rules tree and scrollbar
        self.rules_tree.pack(side='left', fill='both', expand=True)
        rules_scroll.pack(side='right', fill='y')
        
        # Bind double-click event
        self.rules_tree.bind('<Double-1>', self.on_rule_selected)
        
        # Rules actions frame
        rules_actions = ttk.Frame(rules_frame)
        rules_actions.pack(fill='x', padx=5, pady=5)
        
        ttk.Button(rules_actions, text="Show Topics",
                command=lambda: self.show_rule_topics(
                    self.rules_tree.item(self.rules_tree.selection()[0])['values'][0]
                    if self.rules_tree.selection() else None
                )).pack(side='left', padx=5)
        
        ttk.Button(rules_actions, text="Verify Sync",
                command=self.verify_data_sync).pack(side='left', padx=5)
        
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
        
        self.status_tree.column("timestamp", width=150)
        self.status_tree.column("rule", width=150)
        self.status_tree.column("records", width=100)
        self.status_tree.column("success", width=100)
        self.status_tree.column("errors", width=100)
        
        # Add scrollbar to status tree
        status_scroll = ttk.Scrollbar(status_frame, orient="vertical", command=self.status_tree.yview)
        self.status_tree.configure(yscrollcommand=status_scroll.set)
        
        self.status_tree.pack(side='left', fill='both', expand=True)
        status_scroll.pack(side='right', fill='y')

        # Control frame
        control_frame = ttk.Frame(frame)
        control_frame.pack(fill='x', padx=10, pady=5)
        
        # Add diagnostic check button
        ttk.Button(control_frame, text="Diagnostic Check", 
                command=lambda: messagebox.showinfo(
                    "System Check", 
                    self.test_complete_workflow()
                )).pack(side='left', padx=5)
        
        # Left side buttons
        left_buttons_frame = ttk.Frame(control_frame)
        left_buttons_frame.pack(side='left')
        
        ttk.Button(left_buttons_frame, text="Refresh Status", 
                command=self.refresh_monitoring).pack(side='left', padx=5)
        ttk.Button(left_buttons_frame, text="View Details", 
                command=self.show_sync_details).pack(side='left', padx=5)
        
        # Right side buttons
        right_buttons_frame = ttk.Frame(control_frame)
        right_buttons_frame.pack(side='right')
        
        ttk.Button(right_buttons_frame, text="Verify Sync", 
                command=self.verify_data_sync).pack(side='left', padx=5)
        ttk.Button(right_buttons_frame, text="Start Monitoring", 
                command=self.start_monitoring).pack(side='left', padx=5)
        ttk.Button(right_buttons_frame, text="Stop Monitoring", 
                command=self.stop_monitoring).pack(side='left', padx=5)
        
        # Status bar
        status_bar = ttk.Frame(frame)
        status_bar.pack(fill='x', padx=10, pady=5)
        
        # Connection status indicators frame
        conn_status_frame = ttk.Frame(status_bar)
        conn_status_frame.pack(side='left')
        
        # Source status with icon
        source_frame = ttk.Frame(conn_status_frame)
        source_frame.pack(side='left', padx=10)
        self.source_status = ttk.Label(source_frame, text="Source: Not Connected")
        self.source_status.pack(side='left')
        self.source_indicator = tk.Canvas(source_frame, width=12, height=12)
        self.source_indicator.pack(side='left', padx=5)
        
        # Sink status with icon
        sink_frame = ttk.Frame(conn_status_frame)
        sink_frame.pack(side='left', padx=10)
        self.sink_status = ttk.Label(sink_frame, text="Sink: Not Connected")
        self.sink_status.pack(side='left')
        self.sink_indicator = tk.Canvas(sink_frame, width=12, height=12)
        self.sink_indicator.pack(side='left', padx=5)
        
        # Initialize monitoring state
        self.monitoring_active = False
        self.update_monitoring_status()
        
        # Update initial status indicators
        self.update_connection_indicators()
        
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
            
            # Get deployed rules
            c.execute('''SELECT rule_name, source_type, source_table, 
                        target_label, status, kafka_topic
                        FROM deployed_rules''')
            
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
            else:
                # No deployed rules
                self.status_tree.insert('', 'end', values=(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'No Data',
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
        
    def verify_data_sync(self):
        """Verify data synchronization between source and Neo4j"""
        try:
            # Get current mapping rule
            selected = self.rules_tree.selection()
            if not selected:
                messagebox.showwarning("Warning", "Please select a mapping rule to verify")
                return
                
            rule_name = self.rules_tree.item(selected[0])['values'][0]
            
            # Load mapping configuration
            with open(f'mappings/{rule_name}.json', 'r') as f:
                mapping = json.load(f)
                
            # Check source data
            source_count = self.get_source_count(mapping)
            
            # Check Neo4j data
            target_count = self.get_neo4j_count(mapping)
            
            # Create verification window
            verify_window = tk.Toplevel(self.root)
            verify_window.title("Data Sync Verification")
            verify_window.geometry("400x300")
            
            # Add verification details
            text = tk.Text(verify_window, wrap=tk.WORD, padx=10, pady=10)
            text.pack(fill='both', expand=True)
            
            text.insert('end', f"Rule: {rule_name}\n\n")
            text.insert('end', f"Source Records: {source_count}\n")
            text.insert('end', f"Neo4j Nodes: {target_count}\n")
            text.insert('end', f"\nSync Status: ")
            
            if source_count == target_count:
                text.insert('end', "✓ Fully Synced\n")
            else:
                text.insert('end', f"⚠ Pending Sync\n")
                text.insert('end', f"Records remaining: {source_count - target_count}\n")
            
            text.configure(state='disabled')
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to verify data sync: {str(e)}")

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

    def start_monitoring(self):
        """Start monitoring Kafka topics"""
        try:
            # Validate Kafka configuration first
            is_valid, message = self.validate_kafka_config()
            if not is_valid:
                messagebox.showerror("Configuration Error", message)
                return False

            if not self.monitoring_active:
                # Check database status before starting
                if not self.check_database_status():
                    messagebox.showerror("Error", "Failed to initialize monitoring database")
                    return False
                    
                self.monitoring_active = True
                self.monitor_thread = threading.Thread(target=self.monitor_kafka_topics, daemon=True)
                self.monitor_thread.start()
                self.update_monitoring_status()
                return True

        except Exception as e:
            self.logger.error(f"Failed to start monitoring: {str(e)}")
            messagebox.showerror("Error", f"Failed to start monitoring: {str(e)}")
            return False

    def stop_monitoring(self):
        """Stop monitoring Kafka topics"""
        self.monitoring_active = False
        self.update_monitoring_status()


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


    def update_monitoring_status(self):
        """Update monitoring status indicators"""
        if self.monitoring_active:
            self.source_status.config(text="Source: Connected", foreground='green')
            self.sink_status.config(text="Sink: Connected", foreground='green')
        else:
            self.source_status.config(text="Source: Not Connected", foreground='red')
            self.sink_status.config(text="Sink: Not Connected", foreground='red')

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

    def update_sync_status(self, topic, data):
        """Update sync status in database"""
        try:
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            c.execute('''INSERT INTO integration_status 
                        (timestamp, rule_name, records_processed, success_count, error_count)
                        VALUES (datetime('now'), ?, ?, ?, ?)''',
                    (data.get('rule_name', 'Unknown'),
                    data.get('records_processed', 0),
                    data.get('success_count', 0),
                    data.get('error_count', 0)))
            
            conn.commit()
            conn.close()
            
            # Update UI in main thread
            self.root.after(0, self.update_status_tree)
            
        except Exception as e:
            self.logger.error(f"Failed to update sync status: {str(e)}")

    def show_sync_details(self):
        """Show detailed sync information"""
        selected = self.status_tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Please select a status entry to view details")
            return
            
        # Get selected status
        values = self.status_tree.item(selected[0])['values']
        
        # Create details window
        details = tk.Toplevel(self.root)
        details.title("Sync Details")
        details.geometry("500x400")
        
        # Add details
        text = tk.Text(details, wrap=tk.WORD, padx=10, pady=10)
        text.pack(fill='both', expand=True)
        
        text.insert('end', f"Timestamp: {values[0]}\n")
        text.insert('end', f"Rule Name: {values[1]}\n")
        text.insert('end', f"Records Processed: {values[2]}\n")
        text.insert('end', f"Success Count: {values[3]}\n")
        text.insert('end', f"Error Count: {values[4]}\n")
        
        text.configure(state='disabled')
        


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
        """Check if Kafka service is running and accessible"""
        try:
            status = {
                'zookeeper': False,
                'kafka': False,
                'schema_registry': False,
                'connect': False
            }
            
            # Check Kafka broker
            try:
                producer = Producer({
                    'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                    'socket.timeout.ms': 5000,
                    'request.timeout.ms': 5000
                })
                # Use flush instead of close
                producer.flush(timeout=5)
                # Proper cleanup
                del producer
                status['kafka'] = True
            except Exception as e:
                self.logger.error(f"Kafka check failed: {str(e)}")

            # Rest of the checks...
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
            # Validate Kafka configuration first
            is_valid, message = self.validate_kafka_config()
            if not is_valid:
                messagebox.showerror("Configuration Error", message)
                return False

            current = self.mapping_rule_var.get()
            if not current:
                messagebox.showwarning("Warning", "Please select a mapping rule to deploy")
                return False
                
            # Load mapping rule
            mapping_file = f"mappings/{current}.json"
            if not os.path.exists(mapping_file):
                raise Exception("Mapping file not found")
                
            with open(mapping_file, 'r') as f:
                mapping = json.load(f)
            
            # Create topic names        
            source_topic = f"{self.kafka_entries['topic_prefix'].get()}_{mapping['source']['table']}_source"
            sink_topic = f"{self.kafka_entries['topic_prefix'].get()}_{mapping['source']['table']}_sink"
            mapping_topic = f"{self.kafka_entries['topic_prefix'].get()}_mappings"
            
            # Test Kafka connection first
            if not self.test_kafka_connection_silent():
                raise Exception("Kafka is not connected. Please check your Kafka configuration.")

            # Create topics if they don't exist
            from confluent_kafka.admin import AdminClient, NewTopic
            admin_client = AdminClient({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get()
            })

            existing_topics = admin_client.list_topics().topics
            topics_to_create = []

            for topic in [source_topic, sink_topic, mapping_topic]:
                if topic not in existing_topics:
                    topics_to_create.append(NewTopic(
                        topic,
                        num_partitions=1,
                        replication_factor=1
                    ))

            if topics_to_create:
                fs = admin_client.create_topics(topics_to_create)
                for topic, f in fs.items():
                    try:
                        f.result(timeout=5)  # Wait for topic creation
                    except Exception as e:
                        raise Exception(f"Failed to create topic {topic}: {str(e)}")

            # Add topic information to mapping
            mapping['kafka'] = {
                'source_topic': source_topic,
                'sink_topic': sink_topic,
                'mapping_topic': mapping_topic
            }
                
            # Deploy to Kafka
            producer = Producer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'client.id': 'data_integration_deploy',
                'acks': 'all',
                'retries': 3,
                'retry.backoff.ms': 1000,
                'delivery.timeout.ms': 10000,
                'request.timeout.ms': 5000
            })
            
            # Send mapping configuration
            producer.produce(
                mapping_topic,
                key=current.encode('utf-8'),
                value=json.dumps(mapping).encode('utf-8')
            )
            producer.flush(timeout=10)
                
            # Update monitoring database
            conn = sqlite3.connect('monitoring.db')
            c = conn.cursor()
            
            # Store deployment information
            c.execute('''INSERT OR REPLACE INTO deployed_rules 
                        (rule_name, source_type, source_table, target_label, kafka_topic, status, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))''',
                    (current, 
                    mapping['source']['type'],
                    mapping['source']['table'],
                    mapping['target']['label'],
                    json.dumps({
                        'source': source_topic,
                        'sink': sink_topic,
                        'mapping': mapping_topic
                    }),
                    'Deployed'))

            # Add initial monitoring entry
            c.execute('''INSERT INTO integration_status 
                        (timestamp, rule_name, records_processed, success_count, error_count)
                        VALUES (datetime('now'), ?, 0, 0, 0)''',
                    (current,))
            
            conn.commit()
            conn.close()
            
            # Refresh monitoring display
            self.refresh_monitoring()
                
            # Show success message with topics
            messagebox.showinfo("Success", 
                f"Mapping rule '{current}' deployed successfully\n\n"
                f"Created Topics:\n"
                f"Source: {source_topic}\n"
                f"Sink: {sink_topic}\n"
                f"Mapping: {mapping_topic}")

            # Verify deployment
            self.verify_deployment(current, {
                'source_topic': source_topic,
                'sink_topic': sink_topic,
                'mapping_topic': mapping_topic
            })
                
        except Exception as e:
            self.logger.error(f"Failed to deploy mapping rule: {str(e)}")
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


    def show_rule_topics(self, rule_name):
        """Show topics for a specific rule"""
        try:
            # Validate Kafka configuration first
            is_valid, message = self.validate_kafka_config()
            if not is_valid:
                messagebox.showerror("Configuration Error", message)
                return

            if not rule_name:
                messagebox.showwarning("Warning", "Please select a mapping rule")
                return
            
            # Create topics window
            topics_window = tk.Toplevel(self.root)
            topics_window.title(f"Kafka Topics for {rule_name}")
            topics_window.geometry("800x500")
            
            # Create Treeview for topics
            topics_tree = ttk.Treeview(topics_window, 
                columns=("type", "name", "partitions", "messages", "status"),
                show='headings',
                height=10)
                
            # Configure columns
            topics_tree.heading("type", text="Topic Type")
            topics_tree.heading("name", text="Topic Name")
            topics_tree.heading("partitions", text="Partitions")
            topics_tree.heading("messages", text="Message Count")
            topics_tree.heading("status", text="Status")
            
            topics_tree.column("type", width=100)
            topics_tree.column("name", width=300)
            topics_tree.column("partitions", width=100)
            topics_tree.column("messages", width=120)
            topics_tree.column("status", width=100)
            
            # Add scrollbar
            scrollbar = ttk.Scrollbar(topics_window, orient="vertical", command=topics_tree.yview)
            topics_tree.configure(yscrollcommand=scrollbar.set)
            
            topics_tree.pack(side='left', fill='both', expand=True)
            scrollbar.pack(side='right', fill='y')

            # Get topic information
            prefix = self.kafka_entries['topic_prefix'].get()
            topics = {
                'Source': f"{prefix}_{rule_name}_source",
                'Sink': f"{prefix}_{rule_name}_sink",
                'Mapping': f"{prefix}_mappings"
            }


            admin_client = AdminClient({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get()
            })
            
            topic_metadata = admin_client.list_topics().topics
            
            # Create consumer to get offsets
            consumer = Consumer({
                'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get(),
                'group.id': f"{self.kafka_entries['group_id'].get()}_monitor"
            })
            
            for topic_type, topic_name in topics.items():
                if topic_name in topic_metadata:
                    metadata = topic_metadata[topic_name]
                    
                    # Calculate message count using watermarks
                    message_count = 0
                    try:
                        for partition in metadata.partitions:
                            low, high = consumer.get_watermark_offsets(
                                TopicPartition(topic_name, partition), 
                                timeout=5.0
                            )
                            message_count += high - low
                    except KafkaException:
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
                    
            consumer.close()
            admin_client.close()

            
            # Add buttons frame
            button_frame = ttk.Frame(topics_window)
            button_frame.pack(fill='x', padx=10, pady=5)
            
            # Refresh button
            ttk.Button(button_frame, text="Refresh", 
                command=lambda: self.refresh_topic_view(topics_tree, rule_name)).pack(side='left', padx=5)
                
            # View Messages button
            ttk.Button(button_frame, text="View Messages", 
                command=lambda: self.show_topic_messages(
                    topics_tree.item(topics_tree.selection()[0])['values'][1] 
                    if topics_tree.selection() else None
                )).pack(side='left', padx=5)
                
        except Exception as e:
            self.logger.error(f"Failed to show topics: {str(e)}")
            messagebox.showerror("Error", f"Failed to show topic details: {str(e)}")

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
            topics_window.geometry("800x400")
            
            # Create Treeview for topics
            topics_tree = ttk.Treeview(topics_window, 
                columns=("type", "name", "partitions", "messages", "status"),
                show='headings',
                height=10)
                
            # Configure columns
            topics_tree.heading("type", text="Topic Type")
            topics_tree.heading("name", text="Topic Name")
            topics_tree.heading("partitions", text="Partitions")
            topics_tree.heading("messages", text="Message Count")
            topics_tree.heading("status", text="Status")
            
            topics_tree.column("type", width=100)
            topics_tree.column("name", width=300)
            topics_tree.column("partitions", width=100)
            topics_tree.column("messages", width=120)
            topics_tree.column("status", width=100)
            
            # Add scrollbar
            scrollbar = ttk.Scrollbar(topics_window, orient="vertical", command=topics_tree.yview)
            topics_tree.configure(yscrollcommand=scrollbar.set)
            
            # Pack the tree and scrollbar
            topics_tree.pack(side='left', fill='both', expand=True)
            scrollbar.pack(side='right', fill='y')

            # Get topic information
            prefix = self.kafka_entries['topic_prefix'].get()
            topics = {
                'Source': f"{prefix}_{rule_name}_source",
                'Sink': f"{prefix}_{rule_name}_sink",
                'Mapping': f"{prefix}_mappings"
            }
            
            try:
                # Get topic metadata from Kafka
                from confluent_kafka.admin import AdminClient
                admin_client = AdminClient({
                    'bootstrap.servers': self.kafka_entries['bootstrap_servers'].get()
                })
                
                topic_metadata = admin_client.list_topics().topics
                
                for topic_type, topic_name in topics.items():
                    if topic_name in topic_metadata:
                        metadata = topic_metadata[topic_name]
                        
                        # Calculate message count
                        message_count = 0
                        for partition in metadata.partitions.values():
                            message_count += partition.high_watermark - partition.low_watermark
                        
                        status = "Active" if not metadata.error else "Error"
                        
                        topics_tree.insert('', 'end', values=(
                            topic_type,
                            topic_name,
                            len(metadata.partitions),
                            message_count,
                            status
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

            # Add buttons frame
            button_frame = ttk.Frame(topics_window)
            button_frame.pack(fill='x', padx=10, pady=5)
            
            # Refresh button
            ttk.Button(button_frame, text="Refresh",
                    command=lambda: self.refresh_topic_view(topics_window, topics_tree, rule_name)).pack(side='left', padx=5)
            
            # View Messages button
            ttk.Button(button_frame, text="View Messages",
                    command=lambda: self.view_topic_messages(topics_tree)).pack(side='left', padx=5)
            
            # Close button
            ttk.Button(button_frame, text="Close",
                    command=topics_window.destroy).pack(side='right', padx=5)
                
        except Exception as e:
            self.logger.error(f"Failed to show topics: {str(e)}")
            messagebox.showerror("Error", f"Failed to show topic details: {str(e)}")

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
        

if __name__ == "__main__":
    app = DataIntegrationIDE()
    app.root.mainloop()
            