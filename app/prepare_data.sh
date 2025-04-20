#!/bin/bash

source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python)

unset PYSPARK_PYTHON

# DOWNLOAD a.parquet or any parquet file before you run this
[ ! -f "a.parquet" ] &&
    echo "Downloading a.parquet" &&
    curl -s -L -o a.parquet.zip "https://www.kaggle.com/api/v1/datasets/download/jjinho/wikipedia-20230701?fileName=a.parquet" &&
    unzip a.parquet.zip &&
    rm a.parquet.zip

hdfs dfs -put -f a.parquet / &&
    spark-submit prepare_data.py &&
    echo "Putting data to hdfs" &&
    hdfs dfs -put data / &&
    hdfs dfs -ls /data &&
    hdfs dfs -ls /index/data &&
    echo "done data preparation!"
