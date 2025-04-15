#!/bin/bash

if [[ ! -f cassandra_lib.zip ]]; then
    mkdir -p cassandra_lib
    pip install cassandra-driver -t cassandra_lib
    (cd cassandra_lib && zip -qr ../cassandra_lib.zip .)
    rm -rf cassandra_lib
fi

source .venv/bin/activate

python app.py

hdfs dfs -rm -r -f /tmp/index || true

hadoop jar $(find /usr/lib /opt /usr/local -name hadoop-streaming*.jar 2>/dev/null | head -n 1) \
    -input ${1:-/index/data} \
    -output /tmp/index \
    -mapper mapreduce/mapper1.py \
    -reducer mapreduce/reducer1.py \
    -file mapreduce/mapper1.py \
    -file mapreduce/reducer1.py \
    -file cassandra_lib.zip
