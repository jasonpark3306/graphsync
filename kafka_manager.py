import json
import logging
import socket
import requests
import threading
import time
from datetime import datetime
from tkinter import messagebox
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

class KafkaManager:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.monitoring_active = False

    def test_kafka_connection(self):
        """Test Kafka connection using confluent-kafka with proper error handling"""
        try:
            bootstrap_servers = self.config['bootstrap_servers']
            self.logger.info(f"Testing Kafka connection to {bootstrap_servers}")
            
            # First test basic socket connection
            host, port = bootstrap_servers.split(':')
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, int(port)))
            sock.close()
            
            if result != 0:
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
                'group.id': self.config['group_id'],
                'auto.offset.reset': self.config['auto_offset_reset'],
                'socket.timeout.ms': 5000,
                'session.timeout.ms': 6000
            }
            
            consumer = Consumer(consumer_config)
            # Try to list topics to verify connection
            topics = consumer.list_topics(timeout=5)
            if not topics:
                raise ConnectionError("Could not retrieve topic list from broker")
            consumer.close()
            
            return True
                
        except Exception as e:
            self.logger.error(f"Kafka connection error: {str(e)}")
            return False

    def test_kafka_connection_silent(self):
        """Test Kafka connection without showing messages"""
        producer = None
        try:
            bootstrap_servers = self.config['bootstrap_servers']
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

    def check_kafka_service_status(self):
        """Check if Kafka service is running and accessible"""
        try:
            status = {
                'zookeeper': False,
                'kafka': False,
                'schema_registry': False,
                'connect': False
            }
            
            # Check Zookeeper
            try:
                zookeeper = self.config['zookeeper']
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
                producer = Producer({
                    'bootstrap.servers': self.config['bootstrap_servers'],
                    'socket.timeout.ms': 5000
                })
                producer.flush(timeout=5)
                status['kafka'] = True
            except Exception as e:
                self.logger.error(f"Kafka check failed: {str(e)}")
            
            # Check Schema Registry
            try:
                schema_registry_url = self.config['schema_registry']
                response = requests.get(f"{schema_registry_url}/subjects", timeout=5)
                status['schema_registry'] = response.status_code == 200
            except Exception as e:
                self.logger.error(f"Schema Registry check failed: {str(e)}")
            
            # Check Kafka Connect
            try:
                connect_url = self.config['connect_rest']
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

    def deploy_to_kafka(self, mapping_data):
        """Deploy mapping rules to Kafka"""
        producer = None
        try:
            # First, do a comprehensive Kafka check
            if not self.test_kafka_connection_silent():
                raise Exception("Kafka connection failed")

            # Create producer with robust configuration
            producer_config = {
                'bootstrap.servers': self.config['bootstrap_servers'],
                'client.id': 'data_integration_deploy',
                'acks': 'all',
                'retries': 3,
                'retry.backoff.ms': 1000,
                'delivery.timeout.ms': 10000,
                'request.timeout.ms': 5000
            }

            topic = f"{self.config['topic_prefix']}_mappings"
            producer = Producer(producer_config)

            # Send mapping rules
            producer.produce(
                topic,
                key=f"{mapping_data['source']['table']}".encode('utf-8'),
                value=json.dumps(mapping_data).encode('utf-8')
            )
            
            # Wait for delivery
            remaining = producer.flush(timeout=5)
            if remaining > 0:
                raise Exception(f"Failed to flush all messages. {remaining} messages remaining.")

            # Verify the deployment
            if self.verify_kafka_mapping(topic, mapping_data['source']['table']):
                return True
            else:
                raise Exception("Failed to verify mapping deployment")

        except Exception as e:
            self.logger.error(f"Deployment failed: {str(e)}")
            return False
            
        finally:
            # Clean up producer
            if producer is not None:
                producer.flush()
                del producer

    def verify_kafka_mapping(self, topic, source_table):
        """Verify the mapping was properly deployed"""
        consumer = None
        try:
            consumer = Consumer({
                'bootstrap.servers': self.config['bootstrap_servers'],
                'group.id': f"{self.config['group_id']}_verify",
                'auto.offset.reset': 'earliest',
                'session.timeout.ms': 6000,
            })

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

    def ensure_topic_exists(self, topic_name):
        """Ensure the topic exists, create if it doesn't"""
        try:
            admin_client = AdminClient({
                'bootstrap.servers': self.config['bootstrap_servers']
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

    def monitor_topics(self):
        """Monitor Kafka topics and update status"""
        try:
            admin_client = AdminClient({
                'bootstrap.servers': self.config['bootstrap_servers']
            })
            
            # Get topic list
            topics = admin_client.list_topics(timeout=10)
            
            topic_status = {}
            
            for topic_name, topic_metadata in topics.topics.items():
                if topic_name.startswith(self.config['topic_prefix']):
                    # Get message count and offset information
                    message_count = 0
                    last_offset = 0
                    
                    for partition in topic_metadata.partitions.values():
                        last_offset = max(last_offset, partition.high_watermark)
                        message_count += partition.high_watermark - partition.low_watermark
                    
                    topic_status[topic_name] = {
                        'message_count': message_count,
                        'last_offset': last_offset,
                        'last_updated': datetime.now().isoformat()
                    }
            
            return topic_status
            
        except Exception as e:
            self.logger.error(f"Failed to monitor topics: {str(e)}")
            return {}

    def start_monitoring(self):
        """Start monitoring Kafka topics"""
        if not self.monitoring_active:
            self.monitoring_active = True
            self.monitor_thread = threading.Thread(target=self._monitor_kafka_topics, daemon=True)
            self.monitor_thread.start()

    def stop_monitoring(self):
        """Stop monitoring Kafka topics"""
        self.monitoring_active = False
        if hasattr(self, 'monitor_thread'):
            self.monitor_thread.join(timeout=5)

    def _monitor_kafka_topics(self):
        """Background thread to monitor Kafka topics"""
        consumer = None
        try:
            consumer = Consumer({
                'bootstrap.servers': self.config['bootstrap_servers'],
                'group.id': f"{self.config['group_id']}_monitor",
                'auto.offset.reset': 'latest'
            })
            
            # Subscribe to source and sink topics
            source_topic = f"{self.config['topic_prefix']}_source"
            sink_topic = f"{self.config['topic_prefix']}_sink"
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
                    # Emit monitoring data
                    self.emit_monitoring_data(msg.topic(), data)
                except:
                    continue
                    
        except Exception as e:
            self.logger.error(f"Monitoring error: {str(e)}")
            self.monitoring_active = False
            
        finally:
            if consumer is not None:
                consumer.close()

    def emit_monitoring_data(self, topic, data):
        """Emit monitoring data for processing"""
        # This method should be implemented by the user of this class
        pass

    def check_kafka_deployment_readiness(self):
        """Comprehensive check of Kafka deployment prerequisites"""
        try:
            # Test basic connection
            if not self.test_kafka_connection_silent():
                return {'ready': False, 'message': 'Cannot connect to Kafka broker'}

            # Check service status
            service_status = self.check_kafka_service_status()
            if not service_status['kafka']:
                return {'ready': False, 'message': 'Kafka broker is not running'}

            # Test topic creation permissions
            test_topic = f"{self.config['topic_prefix']}_test"
            if not self.ensure_topic_exists(test_topic):
                return {'ready': False, 'message': 'Cannot create topics'}

            return {'ready': True, 'message': 'Kafka is ready for deployment'}

        except Exception as e:
            return {
                'ready': False,
                'message': f"Kafka is not fully ready: {str(e)}"
            }