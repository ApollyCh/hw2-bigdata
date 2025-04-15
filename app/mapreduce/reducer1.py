#!/usr/bin/env python3

import sys, math
sys.path.insert(0, "cassandra_lib.zip")

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel

c = Cluster(["cassandra-server"])
s = c.connect("search_index")

t_i = {}
d_l = {}
t = {}
d_set = set()

for l in sys.stdin:
    l = l.strip()
    if not l:
        continue
    p = l.split("\t", 2)
    if len(p) < 3:
        continue

    k, d, v = p
    try:
        d = int(d)
    except:
        continue

    if k == "@@TIT":
        t[d] = v
    elif k == "@@LEN":
        d_l[d] = int(v)
    elif k == "@@ID":
        d_set.add(d)
    else:
        tf = int(v)
        if k not in t_i:
            t_i[k] = {}
        t_i[k][d] = t_i[k].get(d, 0) + tf

n = len(d_set)
avg = sum(d_l.get(x, 0) for x in d_set) / n if n else 0.0

s.execute("INSERT INTO stats (key, value) VALUES (%s, %s)", ("totalDocs", float(n)))
s.execute("INSERT INTO stats (key, value) VALUES (%s, %s)", ("avgDocLength", float(avg)))

stmt_doc = s.prepare("INSERT INTO documents (doc_id, doc_length, title) VALUES (?, ?, ?)")
b = BatchStatement(consistency_level=ConsistencyLevel.ONE)
for i, d in enumerate(d_set, 1):
    b.add(stmt_doc, (d, d_l.get(d, 0), t.get(d, "")))
    if i % 150 == 0:
        s.execute(b)
        b.clear()
if b:
    s.execute(b)

stmt_vocab = s.prepare("INSERT INTO vocabulary (term, df, idf) VALUES (?, ?, ?)")
stmt_idx = s.prepare("INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)")

bv = BatchStatement(consistency_level=ConsistencyLevel.ONE)
bi = BatchStatement(consistency_level=ConsistencyLevel.ONE)
vc = ic = 0

for term, docs in t_i.items():
    df = len(docs)
    idf = math.log((n + 1.0) / (df + 1.0)) if df else 0.0

    bv.add(stmt_vocab, (term, df, idf))
    vc += 1
    if vc % 75 == 0:
        s.execute(bv)
        bv.clear()

    for doc_id, tf in docs.items():
        bi.add(stmt_idx, (term, doc_id, tf))
        ic += 1
        if ic % 75 == 0:
            s.execute(bi)
            bi.clear()

if bv:
    s.execute(bv)
if bi:
    s.execute(bi)
