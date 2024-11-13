from confluent_kafka import Producer, Consumer
from neo4j import GraphDatabase
import psycopg2
import json
import time
from configparser import ConfigParser

# Configuration
def load_config(filename='db.ini'):
    parser = ConfigParser()
    parser.read(filename)
    return parser


class PostgreSQLSource:
    def __init__(self, config):
        self.conn = psycopg2.connect(
            host=config['postgresql']['host'],
            database=config['postgresql']['database'],
            user=config['postgresql']['user'],
            password=config['postgresql']['password'],
            port=config['postgresql']['port']
        )
        self.cursor = self.conn.cursor()
        
    def get_orders(self):
        self.cursor.execute("SELECT id, name FROM orders")
        return self.cursor.fetchall()

class KafkaProducer:
    def __init__(self, config):
        self.producer = Producer({
            'bootstrap.servers': config['kafka']['bootstrap_servers']
        })
        self.topic = config['kafka']['topic_prefix'] + 'orders'

    def delivery_report(self, err, msg):
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def produce_message(self, key, value):
        self.producer.produce(
            self.topic,
            key=str(key).encode('utf-8'),
            value=json.dumps(value).encode('utf-8'),
            callback=self.delivery_report
        )
        self.producer.flush()

class KafkaConsumer:
    def __init__(self, config):
        self.consumer = Consumer({
            'bootstrap.servers': config['kafka']['bootstrap_servers'],
            'group.id': 'orders_group',
            'auto.offset.reset': 'earliest'
        })
        self.topic = config['kafka']['topic_prefix'] + 'orders'
        self.consumer.subscribe([self.topic])

    def consume_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            yield data

class Neo4jSink:
    def __init__(self, config):
        self.driver = GraphDatabase.driver(
            config['neo4j']['url'],
            auth=(config['neo4j']['user'], config['neo4j']['password'])
        )

    def create_order(self, order_id, name):
        with self.driver.session() as session:
            session.run(
                "MERGE (o:Order {id: $id, name: $name})",
                id=order_id, name=name
            )

    def close(self):
        self.driver.close()

def run_source_to_kafka():
    config = load_config()
    
    # Initialize components
    pg_source = PostgreSQLSource(config)
    kafka_producer = KafkaProducer(config)
    
    # Get orders and send to Kafka
    orders = pg_source.get_orders()
    for order_id, name in orders:
        message = {
            'id': order_id,
            'name': name
        }
        kafka_producer.produce_message(order_id, message)
        print(f"Sent order to Kafka: {message}")

def run_kafka_to_neo4j():
    config = load_config()
    
    # Initialize components
    kafka_consumer = KafkaConsumer(config)
    neo4j_sink = Neo4jSink(config)
    
    try:
        # Consume messages and write to Neo4j
        for message in kafka_consumer.consume_messages():
            neo4j_sink.create_order(message['id'], message['name'])
            print(f"Written to Neo4j: {message}")
    finally:
        neo4j_sink.close()

if __name__ == "__main__":
    # Create two separate Python files: one for source and one for sink
    # Source file (postgres_to_kafka.py):
    run_source_to_kafka()
    
    # Sink file (kafka_to_neo4j.py):
    run_kafka_to_neo4j()