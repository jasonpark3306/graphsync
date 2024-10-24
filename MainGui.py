import tkinter as tk
from tkinter import ttk, messagebox
import configparser  # Add this import
import json
import sqlite3

class DataIntegrationIDE:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Data Integration IDE")
        self.current_step = 0
        self.config = configparser.ConfigParser()
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
        """Save current configuration to db.ini"""
        try:
            with open('db.ini', 'w') as configfile:
                self.config.write(configfile)
        except Exception as e:
            messagebox.showerror("Config Error", f"Error saving db.ini: {str(e)}")

        
    def setup_ui(self):
        # Main container
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(pady=10, expand=True)
        
        # Steps
        self.steps = [
            "Source Selection",
            "Connection Config",
            "Target Config",
            "Mapping Rules",
            "Monitoring"
        ]
        
        self.create_steps()

    def create_connection_config(self):
        frame = ttk.Frame(self.notebook)
        
        # PostgreSQL config
        self.postgres_frame = ttk.LabelFrame(frame, text="PostgreSQL Configuration")
        ttk.Label(self.postgres_frame, text="Host:").grid(row=0, column=0)
        self.pg_host = ttk.Entry(self.postgres_frame)
        self.pg_host.grid(row=0, column=1)
        
        # MongoDB config
        self.mongo_frame = ttk.LabelFrame(frame, text="MongoDB Configuration")
        ttk.Label(self.mongo_frame, text="Connection URI:").grid(row=0, column=0)
        self.mongo_uri = ttk.Entry(self.mongo_frame)
        self.mongo_uri.grid(row=0, column=1)
        
        # Test connection button
        ttk.Button(frame, text="Test Connection", 
                command=self.test_connection).pack(pady=10)
        
        return frame

    def create_postgres_config_panel(self):
        """Create PostgreSQL configuration panel"""
        if self.postgres_frame:
            self.postgres_frame.destroy()
            
        self.postgres_frame = ttk.Frame(self.config_frame)
        
        # Load PostgreSQL config
        pg_config = self.config['postgresql']
        
        # Create grid layout for better alignment
        fields = [
            ('Host:', 'host'),
            ('Port:', 'port'),
            ('Database:', 'database'),
            ('User:', 'user'),
            ('Password:', 'password')
        ]
        
        self.pg_entries = {}
        for i, (label, key) in enumerate(fields):
            # Create label with fixed width
            lbl = ttk.Label(self.postgres_frame, text=label, width=10, anchor='e')
            lbl.grid(row=i, column=0, padx=(5,2), pady=5, sticky='e')
            
            # Create entry with fixed width
            entry = ttk.Entry(self.postgres_frame, width=30)
            entry.insert(0, pg_config.get(key, ''))
            entry.grid(row=i, column=1, padx=(2,5), pady=5, sticky='w')
            self.pg_entries[key] = entry

    def create_mongodb_config_panel(self):
        """Create MongoDB configuration panel"""
        if self.mongodb_frame:
            self.mongodb_frame.destroy()
            
        self.mongodb_frame = ttk.Frame(self.config_frame)
        
        # Load MongoDB config
        mongo_config = self.config['mongodb']
        
        # Create grid layout for better alignment
        fields = [
            ('Host:', 'host'),
            ('Port:', 'port'),
            ('Database:', 'database'),
            ('User:', 'user'),
            ('Password:', 'password')
        ]
        
        self.mongo_entries = {}
        for i, (label, key) in enumerate(fields):
            # Create label with fixed width
            lbl = ttk.Label(self.mongodb_frame, text=label, width=10, anchor='e')
            lbl.grid(row=i, column=0, padx=(5,2), pady=5, sticky='e')
            
            # Create entry with fixed width
            entry = ttk.Entry(self.mongodb_frame, width=30)
            entry.insert(0, mongo_config.get(key, ''))
            entry.grid(row=i, column=1, padx=(2,5), pady=5, sticky='w')
            self.mongo_entries[key] = entry

    def create_source_selection(self):
        frame = ttk.Frame(self.notebook)
        
        # Source selection label
        title_label = ttk.Label(frame, text="Select Source Database:", font=('TkDefaultFont', 10))
        title_label.pack(pady=10)
        
        # Radio buttons frame
        radio_frame = ttk.Frame(frame)
        radio_frame.pack(pady=5)
        
        self.source_var = tk.StringVar(value="postgresql")
        self.source_var.trace('w', self.on_source_change)
        
        ttk.Radiobutton(radio_frame, text="PostgreSQL", 
                        variable=self.source_var, 
                        value="postgresql").pack(side='left', padx=10)
        ttk.Radiobutton(radio_frame, text="MongoDB", 
                        variable=self.source_var, 
                        value="mongodb").pack(side='left', padx=10)

        # Configuration frame
        self.config_frame = ttk.LabelFrame(frame, text="Database Configuration")
        self.config_frame.pack(pady=10, padx=20, fill="both", expand=True)

        # Create and initialize database panels
        self.init_database_panels()
        
        # Buttons frame
        button_frame = ttk.Frame(frame)
        button_frame.pack(pady=10, fill='x', padx=20)
        
        ttk.Button(button_frame, text="Test Connection", 
                command=self.test_connection).pack(side='left')
        ttk.Button(button_frame, text="Save Configuration", 
                command=self.save_source_config).pack(side='right')
        
        return frame

    def init_database_panels(self):
        """Initialize both database panels"""
        # Add logging for debugging
        print("Initializing database panels")
        
        # PostgreSQL panel
        self.postgres_frame = ttk.LabelFrame(self.config_frame, text="PostgreSQL Configuration")  # Changed to LabelFrame
        pg_config = self.config['postgresql']
        print("Creating PostgreSQL panel with config:", pg_config)
        fields = [
            ('Host:', 'host'),
            ('Port:', 'port'),
            ('Database:', 'database'),
            ('User:', 'user'),
            ('Password:', 'password')
        ]
        
        self.pg_entries = {}
        for i, (label, key) in enumerate(fields):
            lbl = ttk.Label(self.postgres_frame, text=label, width=10, anchor='e')
            lbl.grid(row=i, column=0, padx=(5,2), pady=5, sticky='e')
            
            entry = ttk.Entry(self.postgres_frame, width=30)
            entry.insert(0, pg_config.get(key, ''))
            entry.grid(row=i, column=1, padx=(2,5), pady=5, sticky='w')
            self.pg_entries[key] = entry

        # MongoDB panel
        self.mongodb_frame = ttk.Frame(self.config_frame)
        mongo_config = self.config['mongodb']
        
        self.mongo_entries = {}
        for i, (label, key) in enumerate(fields):
            lbl = ttk.Label(self.mongodb_frame, text=label, width=10, anchor='e')
            lbl.grid(row=i, column=0, padx=(5,2), pady=5, sticky='e')
            
            entry = ttk.Entry(self.mongodb_frame, width=30)
            entry.insert(0, mongo_config.get(key, ''))
            entry.grid(row=i, column=1, padx=(2,5), pady=5, sticky='w')
            self.mongo_entries[key] = entry

        # Show PostgreSQL panel by default
        self.postgres_frame.pack(fill="both", expand=True, padx=10, pady=10)

    def on_source_change(self, *args):
        """Handle source database selection change"""
        selected = self.source_var.get()
        print(f"Source changed to: {selected}")
        
        try:
            # First unpack both frames
            if hasattr(self, 'postgres_frame'):
                print("Unpacking PostgreSQL frame")
                self.postgres_frame.pack_forget()
            if hasattr(self, 'mongodb_frame'):
                print("Unpacking MongoDB frame")
                self.mongodb_frame.pack_forget()
            
            # Then pack the selected one
            if selected == "postgresql" and hasattr(self, 'postgres_frame'):
                print("Packing PostgreSQL frame")
                self.postgres_frame.pack(fill="both", expand=True, padx=10, pady=10)
            elif selected == "mongodb" and hasattr(self, 'mongodb_frame'):
                print("Packing MongoDB frame")
                self.mongodb_frame.pack(fill="both", expand=True, padx=10, pady=10)
        except Exception as e:
            print(f"Error in source change: {str(e)}")
            
    def create_steps(self):
        """Create notebook tabs for each integration step"""
        # Create frames for each step
        self.frames = {}
        self.frames["Source Selection"] = self.create_source_selection()
        self.frames["Connection Config"] = self.create_connection_config()
        self.frames["Target Config"] = self.create_target_config()
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
        print(f"Saving configuration for: {selected}")
        
        try:
            if selected == "postgresql":
                for key, entry in self.pg_entries.items():
                    self.config['postgresql'][key] = entry.get()
                    print(f"Saving PostgreSQL {key}: {entry.get()}")
            elif selected == "mongodb":
                for key, entry in self.mongo_entries.items():
                    self.config['mongodb'][key] = entry.get()
                    print(f"Saving MongoDB {key}: {entry.get()}")
            ...
        except Exception as e:
            print(f"Save config error: {str(e)}")
            

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
            # Create label with fixed width
            lbl = ttk.Label(neo4j_frame, text=label, width=10, anchor='e')
            lbl.grid(row=i, column=0, padx=(5,2), pady=5, sticky='e')
            
            # Create entry with fixed width
            entry = ttk.Entry(neo4j_frame, width=30)
            entry.insert(0, self.config['neo4j'].get(key, ''))
            entry.grid(row=i, column=1, padx=(2,5), pady=5, sticky='w')
            self.neo4j_entries[key] = entry
        
        # Add test connection button
        ttk.Button(frame, text="Test Neo4j Connection", 
                command=self.test_neo4j_connection).pack(pady=10)
        
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
            messagebox.showinfo("Success", "Neo4j connection successful!")
            
        except Exception as e:
            messagebox.showerror("Connection Error", f"Failed to connect to Neo4j: {str(e)}")
            

    def test_connection(self):
        """Test database connection based on selected source"""
        selected = self.source_var.get()
        print(f"Testing connection for: {selected}")
        
        try:
            if selected == "postgresql":
                import psycopg2
                config = {key: entry.get() for key, entry in self.pg_entries.items()}
                print("PostgreSQL connection config:", config)
                ...
            elif selected == "mongodb":
                from pymongo import MongoClient
                config = {key: entry.get() for key, entry in self.mongo_entries.items()}
                print("MongoDB connection config:", config)
                ...
        except Exception as e:
            print(f"Connection error: {str(e)}")

            
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
        self.kafka_config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "data_integration_group",
            "auto.offset.reset": "earliest"
        }
        
        # Create Kafka topics
        self.source_topic = "source_data"
        self.target_topic = "neo4j_target"


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
            