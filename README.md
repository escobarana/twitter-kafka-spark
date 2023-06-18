# Streaming Pipeline for Twitter Analytics using Apache Kafka and Apache Spark Structured Streaming

Streaming data ingestion and consumption from Twitter API into Apache Kafka and compute the amount of words on each 
tweet using Spark Structured Streaming.


## Important considerations

**As of June 2023, Twitter has changed the access level for the free accounts. Now, you'll need a Basic subscription to 
be able to access to most of the endpoints, including search tweets, which is the endpoint used in this project.**

Consider using another free API instead the Twitter API to test this project.


## Pre-requisites

Install the necessary packages

````bash
pip install -r requirements.txt
````

## Usage

First, go to `kafka_scripts` and run the `01`, `02` and `03` scripts to get kafka started.


```bash
# Produce tweets to your kafka topic
producer getting_started.ini

# Consume the tweets using Spark Structured Streaming and count the amount of words on each tweet
consumer getting_started.ini
```


## Run tests

These tests are also automated with GitHub actions `on push` to the `main` branch.

````bash
pytest
````