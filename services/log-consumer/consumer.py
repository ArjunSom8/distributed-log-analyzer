# services/log-consumer/consumer.py

import sys
from kafka import KafkaConsumer

# Add the 'proto' directory to our Python path so we can import
# the generated log_message_pb2.py file.
sys.path.append('../../proto')
import log_message_pb2

def main():
  """
  Creates a Kafka consumer that listens to the 'logs' topic,
  deserializes incoming messages, and prints them to the console.
  """
  # Create a KafkaConsumer instance.
  # It will connect to the 'logs' topic.
  try:
    consumer = KafkaConsumer(
        'logs', # The topic to subscribe to
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest', # Start reading at the earliest message
        group_id='log-consumer-group-1' # A unique name for the consumer group
    )
  except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    return

  print("Consumer is listening for messages on the 'logs' topic...")

  # The consumer is an iterator that will block and wait for new messages.
  try:
    for message in consumer:
      # The message value is the raw byte string sent by the producer.
      # We need to deserialize it back into a LogMessage object.
      log_entry = log_message_pb2.LogMessage()
      log_entry.ParseFromString(message.value)

      # Now we can access the fields of the log entry
      print("\n--- New Message Received ---")
      print(f"Key: {message.key}")
      print(f"Partition: {message.partition}, Offset: {message.offset}")
      print("Deserialized Log Message:")
      # The protobuf object has a nice string representation
      print(log_entry)

  except KeyboardInterrupt:
    print("Stopping consumer...")
  finally:
    # Cleanly close the consumer connection
    consumer.close()


if __name__ == '__main__':
  main()
