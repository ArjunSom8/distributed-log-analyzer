# services/log-producer/producer.py

import time
import sys
from kafka import KafkaProducer
from google.protobuf.timestamp_pb2 import Timestamp

# We need to add the 'proto' directory to our Python path
# so we can import the generated log_message_pb2.py file.
# This is a bit of a hack for local development, but it works.
# A more robust solution might involve packaging the proto files.
sys.path.append('../../proto')
import log_message_pb2

def create_log_message():
  """Creates a new LogMessage with sample data."""
  log_entry = log_message_pb2.LogMessage()

  # Create a Timestamp object for the current time
  current_time = Timestamp()
  current_time.GetCurrentTime()
  log_entry.timestamp.CopyFrom(current_time)

  # Populate the other fields with sample data
  log_entry.source_host = "app-server-01"
  log_entry.service_name = "api-gateway"
  log_entry.level = "INFO"
  log_entry.message = "User successfully authenticated."

  return log_entry

def main():
  """
  Creates a Kafka producer, sends a serialized LogMessage to the 'logs' topic,
  and then exits.
  """
  # Create a KafkaProducer instance.
  # We connect to 'localhost:9092', which is the port we exposed in docker-compose.
  try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        # Use a lambda to serialize our Protobuf messages to bytes
        value_serializer=lambda v: v.SerializeToString()
    )
  except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    return

  # Create a new log message
  log_to_send = create_log_message()

  # Define the Kafka topic we want to send messages to
  topic = 'logs'

  # Send the message. The .send() method is asynchronous.
  print(f"Sending LogMessage to topic '{topic}':")
  print(log_to_send)
  
  future = producer.send(topic, value=log_to_send)

  # Block for 'synchronous' sends & get metadata
  try:
    record_metadata = future.get(timeout=10)
    print("\nMessage sent successfully!")
    print(f"Topic: {record_metadata.topic}")
    print(f"Partition: {record_metadata.partition}")
    print(f"Offset: {record_metadata.offset}")
  except Exception as e:
    print(f"Error sending message: {e}")

  # Flush any remaining messages and close the producer
  producer.flush()
  producer.close()


if __name__ == '__main__':
  main()
