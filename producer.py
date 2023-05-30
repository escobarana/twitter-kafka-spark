import logging
import os

import tweepy
from kafka import KafkaProducer

"""TWITTER API ACCESS KEYS"""

tw_api_key = os.environ["TWITTER_API_KEY"]
tw_api_secret_key = os.environ["TWITTER_API_SECRET_KEY"]
tw_access_token = os.environ["TWITTER_ACCESS_TOKEN"]
tw_access_token_secret = os.environ["TWITTER_ACCESS_TOKEN_SECRET"]

producer = KafkaProducer(bootstrap_servers="localhost:9092")
search_term = "Data Engineering"
topic_name = os.environ["KAFKA_TOPIC_NAME"]


def twitterAuth():
    """
        Authenticate ourserlves on the Twitter API
    :return: Twitter API object
    """
    # create the authentication object
    authenticate = tweepy.OAuthHandler(tw_api_key, tw_api_secret_key)
    # set the access token and the access token secret
    authenticate.set_access_token(tw_access_token, tw_access_token_secret)
    # create the API object
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    return api


class TweetListener(tweepy.Stream):
    """
    This is a class that inherits from tweepy.StreamListener and overrides on_data/on_error methods.
    """

    def on_data(self, raw_data):
        logging.info(raw_data)
        producer.send(topic_name, value=raw_data)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False if on_data disconnects the stream
            return False

    def start_streaming_tweets(self, search_term):
        self.filter(track=search_term, stall_warnings=True, languages=["en"])


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    twitter_stream = TweetListener(
        tw_api_key, tw_api_secret_key, tw_access_token, tw_access_token_secret
    )
    twitter_stream.start_streaming_tweets(search_term)
