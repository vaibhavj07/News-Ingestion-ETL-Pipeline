from kafka import KafkaConsumer
import csv
import re
from datetime import datetime

# Function to preprocess each message 
def preprocess_message(message, is_header=False):
    if is_header: # to handle duplicate column names being sent. 
        return message

    message = [None if field == 'None' else field for field in message] # To handle for Null Values. Replaces None with None(Null value for Python)

    # Cleaning columns title (1), description (2), content (3)
    for index in [1, 2, 3]:
        if message[index] is not None:
            # Replacing commas with semicolons, new lines with spaces, and remove other special characters. To handle for any errors while loading data into hive. 
            message[index] = message[index].replace(',', ';').replace('\n', ' ').replace('\r', ' ')
            message[index] = re.sub(r'[^\w\s.;-]', '', message[index])

    # Converting 'published_at' to standard TIMESTAMP format (if it's not None and not a header)
    if message[4] is not None and message[4] != 'published_at':
        message[4] = datetime.strptime(message[4], '%Y-%m-%d %H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')

    return message

# Kafka consumer configuration
conf = {
    'bootstrap_servers': 'localhost:9092',
    'auto_offset_reset': 'latest'  # Start consuming from the latest 
}

# Create the Kafka consumer
consumer = KafkaConsumer('News-Api', **conf)

# Output file
output_filename = 'news_data.csv'

# Maximum number of messages to receive in a single run
max_messages = 1000
messages_received = 0

# Flag to indicate if the header has been written to avoid writing headers multiple times
header_written = False

# Continuously poll for new messages
try:
    with open(output_filename, 'w', newline='', encoding='utf-8') as output_file:
        csv_writer = csv.writer(output_file)

        for msg in consumer:
            # Decoding the message value from bytes to string and splitting it based on ','
            received_message = msg.value.decode().split(',')

            # Print the raw received message
            print(f"Raw message received: {received_message}")

            # Check if the header needs to be written
            if not header_written:
                csv_writer.writerow(received_message)
                header_written = True
                continue

            # calling the preprocess_message function to handle the received message
            preprocessed_message = preprocess_message(received_message)

            # Print the preprocessed message
            print(f"Preprocessed message: {preprocessed_message}")

            # Write the preprocessed message to the CSV file
            csv_writer.writerow(preprocessed_message)

            messages_received += 1

            if messages_received >= max_messages:
                break

except KeyboardInterrupt:
    # Handle script interruption
    print("Interrupted, closing consumer.")
finally:
    # Close the consumer
    consumer.close()
    print("Consumer closed.")


