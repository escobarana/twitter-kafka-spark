# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
import os
import re

from pyspark.ml.feature import RegexTokenizer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode, from_json, split, udf
from pyspark.sql.types import FloatType, StringType, StructField, StructType
from textblob import TextBlob


# remove_links
def cleanTweet(tweet: str) -> str:
    """
        Utility function to clean the text in a tweet
    :param tweet: String
    :return: String (tweet cleaned)
    """
    tweet = re.sub(r"http\S+", "", str(tweet))
    tweet = re.sub(r"bit.ly/\S+", "", str(tweet))
    tweet = tweet.strip("[link]")

    # remove users
    tweet = re.sub("(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)", "", str(tweet))
    tweet = re.sub("(@[A-Za-z]+[A-Za-z0-9-_]+)", "", str(tweet))

    # remove puntuation
    my_punctuation = "!\"$%&'()*+,-./:;<=>?[\\]^_`{|}~•@â"
    tweet = re.sub("[" + my_punctuation + "]+", " ", str(tweet))

    # remove number
    tweet = re.sub("([0-9]+)", "", str(tweet))

    # remove hashtag
    tweet = re.sub("(#[A-Za-z]+[A-Za-z0-9-_]+)", "", str(tweet))

    return tweet


def getSubjectivity(tweet: str) -> float:
    """
        Utility function to classify the polarity of a tweet
    :param tweet: String
    :return: Float (subjectivity)
    """
    return TextBlob(tweet).sentiment.subjectivity


def getPolarity(tweet: str) -> float:
    """
        Utility function to classify the polarity of a tweet
    :param tweet: String
    :return: Float (polarity)
    """
    return TextBlob(tweet).sentiment.polarity


def getSentiment(polarity_value: int) -> str:
    """
        Utility function to classify the polarity of a tweet
    :param polarity_value: Int
    :return: String (sentiment)
    """
    if polarity_value < 0:
        return "Negative"
    elif polarity_value == 0:
        return "Neutral"
    else:
        return "Positive"


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("TwitterSentimentAnalysis")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"
        )
        .getOrCreate()
    )

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", os.environ["KAFKA_TOPIC_NAME"])
        .load()
    )

    my_schema = StructType([StructField("text", StringType(), True)])
    # Get only the "text" from the information we receive from Kafka. The text is the tweet produce by a user
    values = df.select(from_json(df.value.cast("string"), my_schema).alias("tweet"))

    df1 = values.select("tweet.*")
    clean_tweets = F.udf(cleanTweet, StringType())
    raw_tweets = df1.withColumn("processed_text", clean_tweets(col("text")))

    subjectivity = F.udf(getSubjectivity, FloatType())
    polarity = F.udf(getPolarity, FloatType())
    sentiment = F.udf(getSentiment, StringType())

    subjectivity_tweets = raw_tweets.withColumn(
        "subjectivity", subjectivity(col("processed_text"))
    )
    polarity_tweets = subjectivity_tweets.withColumn(
        "polarity", polarity(col("processed_text"))
    )
    sentiment_tweets = polarity_tweets.withColumn(
        "sentiment", sentiment(col("polarity"))
    )

    # Create a tokenizer that Filter away tokens with length < 3, and get rid of symbols like $,#,...
    tokenizer = (
        RegexTokenizer()
        .setPattern("[\\W_]+")
        .setMinTokenLength(3)
        .setInputCol("processed_text")
        .setOutputCol("tokens")
    )

    # Tokenize tweets
    tokenized_tweets = tokenizer.transform(sentiment_tweets)

    # en sortie on a
    tweets_df = (
        tokenized_tweets.withColumn("word", explode(split(col("text"), " ")))
        .groupby("word")
        .count()
        .sort("count", ascending=False)
        .filter(col("word").contains("#"))
    )

    # Print Schema in the console for debugging purposes
    tweets_df.printSchema()

    # Print results in the console
    query = tweets_df.writeStream.outputMode("complete").format("console").start()
