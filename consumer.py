import os
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import findspark

findspark.init()
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession

from confluent_kafka import OFFSET_BEGINNING, Consumer


def create_spark_session(app_name: str):
    return SparkSession \
        .builder \
        .appName(app_name) \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()


def transform(kafka_msg):
    words_df = kafka_msg.select(expr("explode(split(value,' ')) as word"))
    return words_df.groupBy("word").count()


# Set up a callback to handle the '--reset' flag.
def reset_offset(consumer, partitions):
    if args.reset:
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)


if __name__ == "__main__":
    # Create a Spark Session
    spark = create_spark_session(app_name="File Streaming Demo")

    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument("config_file", type=FileType("r"))
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser["default"])
    config.update(config_parser["consumer"])

    # Create Consumer instance
    consumer = Consumer(config)
    topic = os.environ["KAFKA_TOPIC_NAME"]
    consumer.subscribe([topic], on_assign=reset_offset)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                print(
                    "Consumed event from topic {topic}: value = {value:12}".format(
                        topic=msg.topic(),
                        value=msg.value().decode("utf-8"),
                    )
                )
                # Perform Spark computations - count number of words and write parquet file
                counts_df = transform(msg)
                # checkpointLocation is needed to store progress information about the streaming job
                word_count_query = (
                    counts_df.writeStream.format("console")
                    .outputMode("complete")
                    .option("checkpointLocation", "chk-point-dir")
                    .start()
                )  # starts background job

                # wait until the background job finishes
                word_count_query.awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
