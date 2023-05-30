import logging
import os
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import time
import tweepy
from confluent_kafka import Producer

"""TWITTER API ACCESS KEYS"""

tw_api_key = os.environ["TWITTER_API_KEY"]
tw_api_secret_key = os.environ["TWITTER_API_SECRET_KEY"]
tw_access_token = os.environ["TWITTER_ACCESS_TOKEN"]
tw_access_token_secret = os.environ["TWITTER_ACCESS_TOKEN_SECRET"]
tw_api_bearer_token = os.environ["TWITTER_BEARER_TOKEN"]

query = "Data Engineering"
topic_name = os.environ["KAFKA_TOPIC_NAME"]


def twitter_auth():
    """
        Authenticate ourselves on the Twitter API
    :return: Twitter API object
    """
    # create the authentication object
    authenticate = tweepy.OAuth1UserHandler(tw_api_key, tw_api_secret_key, tw_access_token, tw_access_token_secret)
    # create the API object - Authentication is automatically done using tweepy library
    api = tweepy.API(authenticate, wait_on_rate_limit=True)

    # If the authentication was successful, this should print the
    # screen name / username of the account
    logging.info(api.verify_credentials().screen_name)
    return api


def delivery_callback(err, msg):
    """
        Optional per-message delivery callback (triggered by poll() or flush())
    when a message has been successfully delivered or permanently failed delivery (after retries).
    """
    if err:
        logging.error("ERROR: Message failed delivery: {}".format(err))
    else:
        print(
            "Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(),
                key=msg.key().decode("utf-8"),
                value=msg.value().decode("utf-8"),
            )
        )


class TweetListener(tweepy.Client):
    """
        This is a class that inherits from tweepy.Client and overrides on_data/on_error/on_timeout methods.
    """

    def on_data(self, raw_data):
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False if on_data disconnects the stream
            return False

    def on_timeout(self):
        logging.info('Snoozing Zzzzzz')

    def get_recent_tweets(self, search_term):
        return self.search_recent_tweets(query=search_term)


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument("config_file", type=FileType("r"))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser["default"])

    # Create Producer instance
    producer = Producer(config)

    # Get tweets and ingest them
    twitter_stream = TweetListener(tw_api_bearer_token)
    tweets = twitter_stream.get_recent_tweets(search_term=query)
    for _ in range(100):
        for tweet in tweets.data:
            producer.produce(topic=topic_name, key=query, value=tweet.text, callback=delivery_callback)
        time.sleep(5)  # wait 5 seconds to start a new search

        # Block until the messages are sent.
        producer.poll(10000)
        producer.flush()
