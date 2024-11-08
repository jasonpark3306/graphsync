import tkinter as tk
from tkinter import ttk, messagebox
import configparser
import json
import logging
import os
import glob
from datetime import datetime
import threading
from database_handlers import DatabaseManager
from kafka_manager import KafkaManager

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
        
        # Initialize managers
        self.db_manager = DatabaseManager()
        self.db_manager.init_monitoring_db()
        
        self.load_config()
        self.setup_ui()
        
        # Initialize Kafka manager after config is loaded
        self.kafka_manager = KafkaManager(self.config['kafka'])

    def setup_ui(self):
        # Main container
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(pady=10, expand=True)
   
        # Steps configuration
        self.steps = [
            "Source Selection",
            "Target Config",
            "Kafka Config",
            "Mapping Rules",
            "Monitoring"
        ]
        
        self.create_steps()
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
        self.target_indicator = tk.Canvas(target_frame, width=12, height=12)
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

    def create_steps(self):
        """Create notebook tabs for each integration step"""
        # Create frames for each step
        self.frames = {}
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
    

    def save_target_config(self):
        """Save Neo4j configuration to db.ini"""
        try:
            if 'neo4j' not in self.config:
                self.config['neo4j'] = {}
                
            # Save Neo4j configuration
            for key, entry in self.neo4j_entries.items():
                self.config['neo4j'][key] = entry.get()
                
            # Save to file
            if self.config.write(open('db.ini', 'w')):
                messagebox.showinfo("Success", "Neo4j configuration saved successfully")
            else:
                raise Exception("Failed to write configuration file")
                
        except Exception as e:
            self.logger.error(f"Failed to save Neo4j configuration: {str(e)}")
            messagebox.showerror("Error", 
                f"Failed to save Neo4j configuration:\n{str(e)}")

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
        
        ttk.Button(button_frame, text="Test Kafka Connection", 
                command=lambda: self.kafka_manager.test_kafka_connection()).pack(side='left', padx=5)
        
        ttk.Button(button_frame, text="Save Kafka Configuration", 
                command=self.save_kafka_config).pack(side='right', padx=5)
        
        ttk.Button(button_frame, text="Check Services Status",
                command=self.show_kafka_status).pack(side='left', padx=5)
        
        return frame


    def save_kafka_config(self):
        """Save Kafka configuration to db.ini"""
        try:
            if 'kafka' not in self.config:
                self.config['kafka'] = {}
                
            # Save Kafka configuration from entries
            for key, entry in self.kafka_entries.items():
                self.config['kafka'][key] = entry.get()
            
            # Save auto offset reset setting
            self.config['kafka']['auto_offset_reset'] = self.auto_offset.get()
            
            # Save to file
            with open('db.ini', 'w') as configfile:
                self.config.write(configfile)
                
            # Reinitialize Kafka manager with new configuration
            self.kafka_manager = KafkaManager(self.config['kafka'])
                
            messagebox.showinfo("Success", "Kafka configuration saved successfully")
                
        except Exception as e:
            self.logger.error(f"Failed to save Kafka configuration: {str(e)}")
            messagebox.showerror("Error", 
                f"Failed to save Kafka configuration:\n{str(e)}")

    def refresh_kafka_config(self):
        """Refresh Kafka configuration after changes"""
        try:
            # Update Kafka manager with current configuration
            kafka_config = {
                key: entry.get() for key, entry in self.kafka_entries.items()
            }
            kafka_config['auto_offset_reset'] = self.auto_offset.get()
            
            self.kafka_manager = KafkaManager(kafka_config)
            return True
        except Exception as e:
            self.logger.error(f"Failed to refresh Kafka configuration: {str(e)}")
            return False

    def get_kafka_config(self):
        """Get current Kafka configuration from entries"""
        config = {}
        for key, entry in self.kafka_entries.items():
            config[key] = entry.get()
        config['auto_offset_reset'] = self.auto_offset.get()
        return config

    def validate_kafka_config(self):
        """Validate Kafka configuration values"""
        required_fields = ['bootstrap_servers', 'topic_prefix', 'group_id']
        
        for field in required_fields:
            if not self.kafka_entries[field].get().strip():
                messagebox.showerror("Validation Error", 
                    f"Field '{field}' cannot be empty")
                return False
                
        # Validate bootstrap servers format
        bootstrap_servers = self.kafka_entries['bootstrap_servers'].get()
        if not all(':' in server for server in bootstrap_servers.split(',')):
            messagebox.showerror("Validation Error", 
                "Bootstrap servers should be in format host:port")
            return False
            
        return True

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

        # Source table selector frame
        table_frame = ttk.Frame(source_frame)
        table_frame.pack(fill='x', padx=5, pady=2)

        ttk.Label(table_frame, text="Table/Collection:").pack(side='left', padx=5)
        self.source_table_combo = ttk.Combobox(table_frame, 
                                            textvariable=self.source_table_var, 
                                            width=30,
                                            state='disabled')
        self.source_table_combo.pack(side='left', padx=5)
        self.source_table_combo.bind('<<ComboboxSelected>>', self.on_table_selected)
        
        # Target frame
        target_frame = ttk.LabelFrame(mapping_frame, text="Target (Neo4j)")
        target_frame.pack(fill='x', padx=5, pady=5)
        
        ttk.Label(target_frame, text="Node Label:").pack(side='left', padx=5)
        self.target_label_entry = ttk.Entry(target_frame, width=30)
        self.target_label_entry.pack(side='left', padx=5)
        
        ttk.Checkbutton(target_frame, 
                        text="Auto-set label from source",
                        variable=self.auto_label_var).pack(side='left', padx=5)
        
        # Columns mapping frame
        columns_frame = ttk.LabelFrame(mapping_frame, text="Column Mapping")
        columns_frame.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Create two-pane view for columns
        columns_paned = ttk.PanedWindow(columns_frame, orient='horizontal')
        columns_paned.pack(fill='both', expand=True, padx=5, pady=5)
        
        # Source columns frame
        source_list_frame = ttk.Frame(columns_paned)
        ttk.Label(source_list_frame, text="Source Columns").pack()
        
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
                command=lambda: self.kafka_manager.deploy_all_mappings()).pack(side='right', padx=5)
        
        return frame

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
        
        # Add scrollbar
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
        
        # Add scrollbar
        status_scroll = ttk.Scrollbar(status_frame, orient="vertical", command=self.status_tree.yview)
        self.status_tree.configure(yscrollcommand=status_scroll.set)
        
        self.status_tree.pack(side='left', fill='both', expand=True)
        status_scroll.pack(side='right', fill='y')
        
        # Control buttons frame
        control_frame = ttk.Frame(frame)
        control_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Button(control_frame, text="Refresh Status", 
                command=self.refresh_monitoring).pack(side='left', padx=5)
        ttk.Button(control_frame, text="Start Monitoring", 
                command=lambda: self.kafka_manager.start_monitoring()).pack(side='right', padx=5)
        ttk.Button(control_frame, text="Stop Monitoring", 
                command=lambda: self.kafka_manager.stop_monitoring()).pack(side='right', padx=5)
        
        return frame

    # Navigation methods
    def previous_step(self):
        if self.current_step > 0:
            self.current_step -= 1
            self.notebook.select(self.current_step)
            self.prev_button.config(state='disabled' if self.current_step == 0 else 'normal')
            self.next_button.config(state='normal')

    def next_step(self):
        if self.current_step < len(self.steps) - 1:
            self.current_step += 1
            self.notebook.select(self.current_step)
            self.next_button.config(state='disabled' if self.current_step == len(self.steps) - 1 else 'normal')
            self.prev_button.config(state='normal')

    # Configuration methods
    def load_config(self):
        """Load configuration from db.ini file"""
        try:
            self.config.read('db.ini')
            
            # Ensure all sections exist
            required_sections = ['postgresql', 'mongodb', 'neo4j', 'kafka']
            for section in required_sections:
                if section not in self.config:
                    self.config[section] = {}
                    
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            self.create_default_config()

    def create_default_config(self):
        """Create default configuration if none exists"""
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
        
        try:
            with open('db.ini', 'w') as configfile:
                self.config.write(configfile)
        except Exception as e:
            self.logger.error(f"Failed to save default config: {str(e)}")
            messagebox.showerror("Error", 
                f"Failed to create default configuration:\n{str(e)}")

    def save_config(self):
        try:
            with open('db.ini', 'w') as configfile:
                self.config.write(configfile)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save config: {str(e)}")

    # Update methods
    def update_connection_status(self, connection_type, is_connected, db_type=None):
        color = "#2ECC71" if is_connected else "#95A5A6"
        
        if connection_type == "source":
            self.source_indicator.delete("all")
            self.source_indicator.create_oval(2, 2, 10, 10, fill=color, outline=color)
            self.source_type_label.config(text=db_type if db_type else "Disconnected")
        elif connection_type == "target":
            self.target_indicator.delete("all")
            self.target_indicator.create_oval(2, 2, 10, 10, fill=color, outline=color)
            self.target_type_label.config(text=db_type if db_type else "Disconnected")
        elif connection_type == "kafka":
            self.kafka_indicator.delete("all")
            self.kafka_indicator.create_oval(2, 2, 10, 10, fill=color, outline=color)
            self.kafka_type_label.config(text=db_type if db_type else "Disconnected")


    # Event handlers and utility methods
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

    def save_source_config(self):
        """Save current source configuration to db.ini"""
        selected = self.source_var.get()
        try:
            if selected == "postgresql":
                for key, entry in self.pg_entries.items():
                    self.config['postgresql'][key] = entry.get()
            elif selected == "mongodb":
                for key, entry in self.mongo_entries.items():
                    self.config['mongodb'][key] = entry.get()
            
            self.save_config()
            messagebox.showinfo("Success", f"{selected.upper()} configuration saved successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save configuration: {str(e)}")

    def test_connection(self):
        """Test database connection based on selected source"""
        selected = self.source_var.get()
        config = {}
        
        if selected == "postgresql":
            config = {key: entry.get() for key, entry in self.pg_entries.items()}
        else:
            config = {key: entry.get() for key, entry in self.mongo_entries.items()}

        result = self.db_manager.test_connection(selected, config)
        if result:
            self.update_connection_status("source", True, selected.upper())
            messagebox.showinfo("Success", f"{selected.upper()} connection successful!")
        else:
            self.update_connection_status("source", False)

    # Mapping methods
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
                    
                self.source_table_var.set(mapping['source']['table'])
                self.target_label_entry.delete(0, tk.END)
                self.target_label_entry.insert(0, mapping['target']['label'])
                
                self.mapped_columns.delete(*self.mapped_columns.get_children())
                
                for source_col, target_col in mapping['columns'].items():
                    self.mapped_columns.insert('', 'end', values=(source_col, target_col))
                    
                # Load source fields
                self.load_source_fields()
                
        except Exception as e:
            self.logger.error(f"Failed to load mapping rule: {str(e)}")
            messagebox.showerror("Error", "Failed to load mapping rule")

    def refresh_monitoring(self):
        """Refresh monitoring data"""
        try:
            # Clear existing items
            self.rules_tree.delete(*self.rules_tree.get_children())
            self.status_tree.delete(*self.status_tree.get_children())
            
            # Get deployed rules
            deployed_rules = self.db_manager.get_deployed_rules()
            
            if deployed_rules:
                for rule in deployed_rules:
                    self.rules_tree.insert('', 'end', values=rule)
                    
                # Get sync status
                sync_status = self.db_manager.get_sync_status()
                if sync_status:
                    for status in sync_status:
                        self.status_tree.insert('', 'end', values=status)
                
        except Exception as e:
            self.logger.error(f"Failed to refresh monitoring: {str(e)}")
            messagebox.showerror("Error", "Failed to refresh monitoring data")

    def show_sync_details(self):
        """Show detailed sync information"""
        selected = self.status_tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Please select a status entry to view details")
            return
            
        values = self.status_tree.item(selected[0])['values']
        
        details = tk.Toplevel(self.root)
        details.title("Sync Details")
        details.geometry("500x400")
        
        text = tk.Text(details, wrap=tk.WORD, padx=10, pady=10)
        text.pack(fill='both', expand=True)
        
        text.insert('end', f"Timestamp: {values[0]}\n")
        text.insert('end', f"Rule Name: {values[1]}\n")
        text.insert('end', f"Records Processed: {values[2]}\n")
        text.insert('end', f"Success Count: {values[3]}\n")
        text.insert('end', f"Error Count: {values[4]}\n")
        
        text.configure(state='disabled')

    def show_kafka_status(self):
        """Display Kafka service status"""
        status = self.kafka_manager.check_kafka_service_status()
        
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
        
        for service, is_running in services.items():
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
        
        ttk.Button(status_window, text="Refresh", 
                command=lambda: self.refresh_kafka_status(status_window)).pack(pady=10)

    def refresh_kafka_status(self, window):
        """Refresh Kafka status window"""
        window.destroy()
        self.show_kafka_status()

    def on_closing(self):
        """Handle application closing"""
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
            # Stop monitoring if active
            if hasattr(self, 'kafka_manager'):
                self.kafka_manager.stop_monitoring()
            self.root.destroy()

    def run(self):
        """Start the application"""
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.root.mainloop()



    def test_neo4j_connection(self):
        """Test Neo4j connection"""
        try:
            config = {key: entry.get() for key, entry in self.neo4j_entries.items()}
            if self.db_manager.test_neo4j_connection(config):
                self.update_connection_status("target", True, "Neo4j")
                messagebox.showinfo("Success", "Neo4j connection successful!")
            else:
                self.update_connection_status("target", False)
                messagebox.showerror("Error", "Failed to connect to Neo4j")
        except Exception as e:
            self.logger.error(f"Neo4j connection test failed: {str(e)}")
            self.update_connection_status("target", False)
            messagebox.showerror("Error", f"Failed to connect to Neo4j: {str(e)}")

    def on_table_selected(self, event=None):
        """Handle table selection and automatically set Neo4j label"""
        selected_table = self.source_table_var.get()
        if not selected_table:
            return
                
        # Only auto-set label if checkbox is checked
        if self.auto_label_var.get():
            self.target_label_entry.delete(0, tk.END)
            self.target_label_entry.insert(0, selected_table.capitalize())
            
        # Load the fields for the selected table
        self.load_table_fields()

    def load_table_fields(self):
        """Load fields from selected table/collection"""
        try:
            # Clear existing items
            for item in self.source_columns.get_children():
                self.source_columns.delete(item)
                    
            selected_table = self.source_table_var.get()
            
            # Get configuration based on source type
            if self.source_var.get() == "postgresql":
                config = {key: entry.get() for key, entry in self.pg_entries.items()}
            else:  # mongodb
                config = {key: entry.get() for key, entry in self.mongo_entries.items()}
            
            # Get fields from database manager
            fields = self.db_manager.load_source_fields(
                self.source_var.get(),
                config,
                selected_table
            )
            
            # Insert fields into source_columns treeview
            for field, dtype in fields:
                self.source_columns.insert('', 'end', values=(field, dtype))
                        
        except Exception as e:
            self.logger.error(f"Failed to load table fields: {str(e)}")
            messagebox.showerror("Error", f"Failed to load fields: {str(e)}")

    def map_all_columns(self):
        """Map all source columns to target properties with same names"""
        try:
            self.mapped_columns.delete(*self.mapped_columns.get_children())
            
            for item in self.source_columns.get_children():
                column = self.source_columns.item(item)['values'][0]
                self.mapped_columns.insert('', 'end', values=(column, column))
        except Exception as e:
            self.logger.error(f"Failed to map all columns: {str(e)}")
            messagebox.showerror("Error", "Failed to map columns")

    def map_selected_columns(self):
        """Map selected source columns to target properties"""
        try:
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
        except Exception as e:
            self.logger.error(f"Failed to map selected columns: {str(e)}")
            messagebox.showerror("Error", "Failed to map selected columns")

    def remove_mapped_columns(self):
        """Remove selected mapped columns"""
        try:
            selected = self.mapped_columns.selection()
            for item in selected:
                self.mapped_columns.delete(item)
        except Exception as e:
            self.logger.error(f"Failed to remove mapped columns: {str(e)}")
            messagebox.showerror("Error", "Failed to remove mapped columns")

    def remove_all_mappings(self):
        """Remove all mapped columns"""
        try:
            self.mapped_columns.delete(*self.mapped_columns.get_children())
        except Exception as e:
            self.logger.error(f"Failed to remove all mappings: {str(e)}")
            messagebox.showerror("Error", "Failed to remove all mappings")

    def get_current_mapping(self):
        """Get current mapping configuration"""
        try:
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
                
            return mapping
        except Exception as e:
            self.logger.error(f"Failed to get current mapping: {str(e)}")
            return None

    def validate_current_mapping(self):
        """Validate current mapping configuration"""
        mapping = self.get_current_mapping()
        if not mapping:
            return False
            
        if not mapping['source']['table']:
            messagebox.showwarning("Warning", "Please select a source table")
            return False
            
        if not mapping['target']['label']:
            messagebox.showwarning("Warning", "Please enter a target label")
            return False
            
        if not mapping['columns']:
            messagebox.showwarning("Warning", "Please map at least one column")
            return False
            
        return True
    


    def save_current_mapping(self):
        """Save current mapping configuration to file"""
        try:
            # Get current mapping rule name
            current = self.mapping_rule_var.get()
            if not current:
                messagebox.showwarning("Warning", "Please select or create a mapping rule first")
                return

            # Validate current mapping
            if not self.validate_current_mapping():
                return

            # Get mapping configuration
            mapping = self.get_current_mapping()
            if not mapping:
                return

            # Create mappings directory if it doesn't exist
            if not os.path.exists('mappings'):
                os.makedirs('mappings')

            # Save mapping to file
            mapping_file = f"mappings/{current}.json"
            try:
                with open(mapping_file, 'w') as f:
                    json.dump(mapping, f, indent=2)
                messagebox.showinfo("Success", f"Mapping rule '{current}' saved successfully")
                
                # Update combo box if new mapping
                if current not in self.mapping_rule_combo['values']:
                    values = list(self.mapping_rule_combo['values'])
                    values.append(current)
                    self.mapping_rule_combo['values'] = values
                
            except Exception as e:
                raise Exception(f"Failed to save mapping file: {str(e)}")

        except Exception as e:
            self.logger.error(f"Failed to save mapping rule: {str(e)}")
            messagebox.showerror("Error", f"Failed to save mapping rule:\n{str(e)}")

    def clear_mapping_form(self):
        """Clear all mapping form fields"""
        try:
            # Clear source table
            self.source_table_var.set('')
            
            # Clear target label
            self.target_label_entry.delete(0, tk.END)
            
            # Clear mapped columns
            self.mapped_columns.delete(*self.mapped_columns.get_children())
            
            # Clear source columns
            self.source_columns.delete(*self.source_columns.get_children())
            
        except Exception as e:
            self.logger.error(f"Failed to clear mapping form: {str(e)}")



    def deploy_selected_mapping(self):
        """Deploy currently selected mapping rule to Kafka"""
        try:
            # Get currently selected mapping rule
            current = self.mapping_rule_var.get()
            if not current:
                messagebox.showwarning("Warning", "Please select a mapping rule to deploy")
                return
                
            # Load mapping rule
            mapping_file = f"mappings/{current}.json"
            if not os.path.exists(mapping_file):
                messagebox.showerror("Error", "Mapping file not found")
                return
                
            with open(mapping_file, 'r') as f:
                mapping = json.load(f)
            
            # Create topic names        
            source_topic = f"{self.kafka_entries['topic_prefix'].get()}_{mapping['source']['table']}_source"
            sink_topic = f"{self.kafka_entries['topic_prefix'].get()}_{mapping['source']['table']}_sink"
            mapping_topic = f"{self.kafka_entries['topic_prefix'].get()}_mappings"
            
            # Test Kafka connection first
            if not self.kafka_manager.test_kafka_connection_silent():
                raise Exception("Kafka is not connected. Please check your Kafka configuration.")

            # Add topic information to mapping
            mapping['kafka'] = {
                'source_topic': source_topic,
                'sink_topic': sink_topic,
                'mapping_topic': mapping_topic
            }
                
            # Deploy to Kafka
            if self.kafka_manager.deploy_to_kafka(mapping):
                # Update monitoring database
                self.db_manager.save_deployment_status(
                    current,
                    mapping['source']['type'],
                    mapping['source']['table'],
                    mapping['target']['label'],
                    {
                        'source': source_topic,
                        'sink': sink_topic,
                        'mapping': mapping_topic
                    }
                )
                
                # Show success message
                messagebox.showinfo("Success", 
                    f"Mapping rule '{current}' deployed successfully\n\n"
                    f"Created Topics:\n"
                    f"Source: {source_topic}\n"
                    f"Sink: {sink_topic}\n"
                    f"Mapping: {mapping_topic}")
                
                # Refresh monitoring display
                self.refresh_monitoring()
            else:
                raise Exception("Failed to deploy mapping to Kafka")
                
        except Exception as e:
            self.logger.error(f"Deployment failed: {str(e)}")
            messagebox.showerror("Deployment Error",
                f"Failed to deploy mapping rule:\n{str(e)}\n\n"
                "Please check:\n"
                "1. Kafka connection\n"
                "2. Mapping configuration\n"
                "3. Topic permissions")

    def verify_deployment(self, rule_name, topics):
        """Verify deployment by checking topics and configurations"""
        try:
            # Check if topics exist
            missing_topics = []
            for topic_type, topic_name in topics.items():
                if not self.kafka_manager.topic_exists(topic_name):
                    missing_topics.append(f"{topic_type}: {topic_name}")
            
            if missing_topics:
                raise Exception(f"Missing topics:\n" + "\n".join(missing_topics))
                
            # Verify mapping in database
            deployed_rules = self.db_manager.get_deployed_rules()
            if not any(rule[0] == rule_name and rule[5] == 'Deployed' for rule in deployed_rules):
                raise Exception("Rule not properly registered in monitoring database")
                
            # Verify mapping in Kafka
            consumer = self.kafka_manager.create_consumer(group_id_suffix='_verify')
            topics['mapping_topic'] = f"{self.kafka_entries['topic_prefix'].get()}_mappings"
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