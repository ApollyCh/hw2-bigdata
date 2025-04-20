#!/usr/bin/env bash

set -euo pipefail

INPUT_PATH=${1:-/index/data}
OUTPUT_PATH=/tmp/index

source .venv/bin/activate
python app.py

if [ ! -f cassandra_driver.zip ]; then
    mkdir -p cassandra_tmp
    CASSANDRA_DRIVER_NO_CYTHON=1 pip install cassandra-driver -t cassandra_tmp/
    (cd cassandra_tmp && zip -qr ../cassandra_driver.zip .)
    rm -rf cassandra_tmp
fi

hdfs dfs -rm -r -f "$OUTPUT_PATH" || true

hadoop jar $(find /usr/lib /opt /usr/local -name 'hadoop-streaming*.jar' 2>/dev/null | head -n 1) \
    -files "mapreduce/mapper1.py,mapreduce/reducer1.py,cassandra_driver.zip" \
    -input "$INPUT_PATH" \
    -output "$OUTPUT_PATH" \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py"

echo "Indexing done!"
