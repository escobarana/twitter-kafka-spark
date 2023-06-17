#!/usr/bin/env python
from pathlib import Path

from setuptools import find_packages, setup

setup(
    name="Apache Kafka and Spark Structured Streaming Data Pipeline using Twitter API Streams",
    version="1.0.0",
    description='Building a streaming data pipeline using Apache Kafka, Apache Spark Structured Streaming and Twitter '
                'API Streams.',
    long_description=(Path(__file__).parent / "README.md").read_text(),
    long_description_content_type="text/markdown",
    author="Ana Escobar",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3 :: Only",
    ],
    keywords="kafka and spark structured streaming pipeline with twitter api streams",
    packages=find_packages(include=["consumer.py", "producer.py", "getting_started.ini", "tests"]),
    python_requires=">=3.5, <4",
)
