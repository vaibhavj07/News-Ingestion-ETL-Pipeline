import pandas as pd
import requests
from kafka import KafkaProducer
import time

# Flag to track whether column names have been sent
column_names_sent = False

def send_to_kafka(dataframe):
    global column_names_sent  # Use the global flag

    # Kafka producer configuration
    topic = "News-Api"
    brokers = "localhost:9092"

    # Create the Kafka producer
    producer = KafkaProducer(bootstrap_servers=brokers)

    if dataframe.empty:
        print("DataFrame is empty. Nothing to send to Kafka.")
        producer.close()
        return

    # Send column names only if they haven't been sent yet
    if not column_names_sent:
        # Convert column names to a comma-separated string so its easier to read and store
        col_names_str = ','.join(dataframe.columns)
        
#Exception handling to handle for any errors. 
        try:
            print(f"Sending column names to Kafka: {col_names_str}")
            producer.send(topic, col_names_str.encode())
            producer.flush()  # Wait until acknowledgment is received from the broker before continuing

            print("Sent column names successfully to Kafka.")
            column_names_sent = True  # Update the flag
        except Exception as e:
            print(f"Error sending column names to Kafka: {e}")

    # Iterate through each row in the DataFrame
    for index, row in dataframe.iterrows():
        try:
            # Convert the row to a comma-separated string so its easier to read and store as a csv
            row_str = ','.join(map(str, row))
            print(f"Sending data to Kafka: {row_str}")

            # Send the row to the Kafka topic
            producer.send(topic, row_str.encode())
            producer.flush()  

            print("Sent successfully to Kafka.")
        except Exception as e:
            print(f"Error sending to Kafka: {e}")

        # Introduce a delay (optional)
        time.sleep(1)

    # Close the Kafka producer
    producer.close()

def get_news(api_key, country='us', category='general', keywords=None, page_size=5):
    base_url = 'https://newsapi.org/v2/top-headlines'
    
    # Iterate through each keyword 
    for q in keywords:
        # Set up the parameters
        params = {
            'apiKey': api_key,
            'country': country,
            'category': category,
            'q': q,
            'pageSize': page_size
        }
        
        # Making the request
        response = requests.get(base_url, params=params)
        
        # Checking if the request was successful (status code 200)
        if response.status_code == 200:
            data = response.json()
            
            # Extract data and creating a DataFrame
            articles = data.get('articles', [])
            
            news_df = pd.DataFrame(articles)
            
            # Checking if 'publishedAt' exists before using it as some responses did not include publishedAt values
            if 'publishedAt' in news_df.columns:
                # Split 'publishedAt' into year, month, day, and day_of_week and it would make the analysis easier further 
                news_df['published_at'] = pd.to_datetime(news_df['publishedAt'])
                news_df['year'] = news_df['published_at'].dt.year
                news_df['month'] = news_df['published_at'].dt.month
                news_df['day'] = news_df['published_at'].dt.day
                news_df['day_of_week'] = news_df['published_at'].dt.day_name()
            else:
                # Assigning None if 'publishedAt' is not present
                news_df['publishedAt'] = None
                news_df['published_at'] = None
                news_df['year'] = None
                news_df['month'] = None
                news_df['day'] = None
                news_df['day_of_week'] = None
            
            # Check if 'source' exists before using it some responses did not include source values
            if 'source' in news_df.columns:
                # Split 'source' column into 'source_id' and 'source_name' -- using lambda function. Source contained two fields, so splitting it in two seperate columns
                news_df['source_id'] = news_df['source'].apply(lambda x: x['id'] if x else None)
                news_df['source_name'] = news_df['source'].apply(lambda x: x['name'] if x else None)
            else:
                # Assign None if 'source' is not present
                news_df['source'] = None
                news_df['source_id'] = None
                news_df['source_name'] = None
            
            columns_to_drop = ['source', 'publishedAt', 'url', 'urlToImage'] # dropping these columns as we dont need them any more. 
            news_df = news_df.drop(columns=columns_to_drop, errors='ignore') # ignoring errors while dropping if the columns dont exist
            
            # as we are storing data as a csv, it was wise to remove any extra ',' in text fields to avoid errors while loading the data in hive
            columns_to_replace_commas = ['title', 'description', 'content'] 
            for column in columns_to_replace_commas:
                if column in news_df.columns:
                    news_df[column] = news_df[column].str.replace(',', '')
            
            # Add a new column for the keyword
            news_df['keyword'] = q
            
            send_to_kafka(news_df)
            
            print("Data sent to Kafka.")
        else:
            print(f"Error: {response.status_code}")

# API key 
api_key = 'da8ea720fb0c47dc8fd98574ec8f0940'

# newsapi parameters
get_news(api_key, country='in', category='sports', keywords=['cricket', 'football', 'ipl', 'isl','world cup','t20','odi','test','premier league'], page_size=100)


