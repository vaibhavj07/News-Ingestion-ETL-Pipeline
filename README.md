# News-Ingestion-ETL-Pipeline
A Robust ETL Pipeline to Ingest News Article from News API and perform analysis on HIVE

Requirements 
-- Zookeper 3.4.14
-- Apache Kafka 4.1.4
-- Kafka Library for Python
-- Pandas
-- Hadoop Stack



Steps
1. Change Directory to Zookeeper Directory
2. Run Zookeper - bin/zkServer.sh start
3. Navigate to Apache Kafka 4.1.4
4. Run Kafka Server - nohup bin/kafka-server-start etc/kafka/server.properties > /dev/null 2>&1 &
5. Create Topic - bin/kafka-topics --create --zookeeper localhost:2182 --partitions 1 --replication-factor 1 --topic News-Api
6. Describe to check if the topic has been created - bin/kafka-topics --describe --zookeeper localhost:2182 --topic News-Api
7. Run The Consumer Script to start listening for data - python newsapi_reciever.py
8. Run The Producer Script to start ingesting data from News API and transmitting data - python newsapi_producer.py
9. Copy the CSV Generated and move it HDFS -  hdfs dfs -copyFromLocal <sourcepath>  <destinationapath>
10. Run Hive and Create DB and Table - DB_and_Table_Create_Statements.hql
11. Run Analysis - Analysis.hql
