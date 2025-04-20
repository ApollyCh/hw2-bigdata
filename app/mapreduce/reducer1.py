#!/usr/bin/env python3
import sys
import math
from collections import defaultdict

sys.path.insert(0, "cassandra_driver.zip")

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel

BATCH_SIZE = 75

cluster = Cluster(["cassandra-server"])
session = cluster.connect("search_index")

stmt_docs = session.prepare("INSERT INTO documents (doc_id, title, doc_length) VALUES (?, ?, ?)")
stmt_index = session.prepare("INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)")
stmt_vocab = session.prepare("INSERT INTO vocabulary (term, df, idf) VALUES (?, ?, ?)")
stmt_stats = session.prepare("INSERT INTO stats (key, value) VALUES (?, ?)")

term_index = defaultdict(dict)
doc_lengths = {}
doc_titles = {}
doc_ids = set()

for line in sys.stdin:
    parts = line.strip().split("\t", 2)
    if len(parts) != 3:
        continue

    key, doc_id_str, val = parts
    doc_id = int(doc_id_str)

    if key == "__META__":
        doc_titles[doc_id] = val
    elif key == "__DOCLEN__":
        doc_lengths[doc_id] = int(val)
        doc_ids.add(doc_id)
    else:
        tf = int(val)
        term_index[key][doc_id] = tf

total_docs = len(doc_ids)
avgdl = sum(doc_lengths.values()) / total_docs if total_docs else 0.0

session.execute(stmt_stats, ("N", float(total_docs)))
session.execute(stmt_stats, ("avgdl", float(avgdl)))

# Вставка: документы
batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
for i, doc_id in enumerate(doc_ids, 1):
    batch.add(stmt_docs, (doc_id, doc_titles.get(doc_id, ""), doc_lengths.get(doc_id, 0)))
    if i % BATCH_SIZE == 0:
        session.execute(batch)
        batch.clear()
if batch:
    session.execute(batch)

# vocabulary + inverted_index
batch_vocab = BatchStatement(consistency_level=ConsistencyLevel.ONE)
batch_index = BatchStatement(consistency_level=ConsistencyLevel.ONE)
count_vocab = count_index = 0

for term, doc_map in term_index.items():
    df = len(doc_map)
    idf = math.log((total_docs + 1.0) / (df + 1.0))

    batch_vocab.add(stmt_vocab, (term, df, idf))
    count_vocab += 1
    if count_vocab % BATCH_SIZE == 0:
        session.execute(batch_vocab)
        batch_vocab.clear()

    for doc_id, tf in doc_map.items():
        batch_index.add(stmt_index, (term, doc_id, tf))
        count_index += 1
        if count_index % BATCH_SIZE == 0:
            session.execute(batch_index)
            batch_index.clear()

if batch_vocab:
    session.execute(batch_vocab)
if batch_index:
    session.execute(batch_index)

print("✅ Indexing complete.")
