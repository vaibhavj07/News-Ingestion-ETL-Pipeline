1. Run Zookeeper service
command : 
cd zookeeper-3.4.14
bin/zkServer.sh start

2. Run Kafka server
command : 
cd ../
cd confluent-4.1.4/
nohup bin/kafka-server-start etc/kafka/server.properties > /dev/null 2>&1 &


3. Describe Topic (Already Created)
# bin/kafka-topics --create --zookeeper localhost:2182 --partitions 1 --replication-factor 1 --topic News-Api
command :
bin/kafka-topics --describe --zookeeper localhost:2182 --topic News-Api


4. Run Consumer : 
command: 
cd final_project/
python newsapi_reciever.py

5. Run Producer:
command:
cd final_project/
python newsapi_producer.py

6. Copy Data to Hdfs 
command: hdfs dfs -copyFromLocal /home/yeshashah052/final_project/news_data.csv /user/yesha/newsapiproject
hdfs dfs -ls /user/yesha/newsapiproject


7. Run Hive/ Create a database
command : 
hive 

create database newsapi_project;

use newsapi_project; 

CREATE TABLE IF NOT EXISTS news_data_table (
    author STRING,
    title STRING,
    description STRING,
    content STRING,
    published_at TIMESTAMP,
    year INT,
    month INT,
    day INT,
    day_of_week STRING,
    source_id STRING,
    source_name STRING,
    keyword STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",  -- Field separator
    "quoteChar"     = "\"", -- Quote character
    "escapeChar"    = "\\", -- Escape character
    "serialization.null.format" = "" 
)
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/yesha/newsapiproject/news_data.csv' INTO TABLE news_data_table;




Running Analysis : 


SELECT source_name, COUNT(*) as article_count
FROM news_data_table
GROUP BY source_name
ORDER BY article_count DESC;


SELECT published_at, COUNT(*) as total_articles
FROM news_data_table
GROUP BY published_at;

SELECT keyword, COUNT(*) as frequency
FROM news_data_table
GROUP BY keyword
ORDER BY frequency DESC;

SELECT day_of_week, COUNT(*) as total_articles
FROM news_data_table
GROUP BY day_of_week
ORDER BY total_articles DESC;

SELECT author, COUNT(*) as total_articles
FROM news_data_table
WHERE author IS NOT NULL AND author != ''
GROUP BY author
ORDER BY total_articles DESC;
