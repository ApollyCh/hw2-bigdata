import sys
import math
import re
from collections import defaultdict

from pyspark import SparkConf, SparkContext
from cassandra.cluster import Cluster

STOPWORDS = {
    "is", "are", "was", "were", "be", "been", "being",
    "am", "it", "this", "that", "these", "those",
    "he", "she", "they", "we", "you", "your", "our",
    "have", "has", "had", "do", "does", "did",
    "will", "would", "shall", "may", "might", "can", "could",
    "as", "what", "which", "who", "whom", "how", "where", "when",
    "again", "more", "most", "very", "too", "just", "not"
}

def tokenize(text):
    return [t for t in re.findall(r'\b\w+\b', text.lower()) if t not in STOPWORDS]

# Read query from arguments
query_text = " ".join(sys.argv[1:]).strip()
if not query_text:
    print("❗ Please provide a query string.")
    sys.exit(1)

query_terms = tokenize(query_text)
if not query_terms:
    print("❗ No valid terms found in query.")
    sys.exit(1)

# Initialize Spark
conf = SparkConf().setAppName("BM25Query")
sc = SparkContext(conf=conf)

# Connect to Cassandra
cluster = Cluster(["cassandra-server"])
session = cluster.connect("search_index")

# Load global stats
N = session.execute("SELECT value FROM stats WHERE key='N'").one().value
avgdl = session.execute("SELECT value FROM stats WHERE key='avgdl'").one().value

# Load vocabulary
vocab_map = {}
idf_map = {}
for term in query_terms:
    row = session.execute("SELECT df, idf FROM vocabulary WHERE term=%s", (term,)).one()
    if row:
        vocab_map[term] = row.df
        idf_map[term] = row.idf

if not vocab_map:
    print("❗ None of the query terms exist in index.")
    sys.exit(0)

# Load relevant entries from inverted index
inverted = []
for term in query_terms:
    if term in vocab_map:
        rows = session.execute("SELECT doc_id, tf FROM inverted_index WHERE term=%s", (term,))
        for row in rows:
            inverted.append((row.doc_id, term, row.tf))

# Load document metadata
doc_info = {}
rows = session.execute("SELECT doc_id, doc_length, title FROM documents")
for row in rows:
    doc_info[row.doc_id] = {
        "length": row.doc_length,
        "title": row.title
    }

# BM25 scoring
k1 = 1.5
b = 0.75

def compute_bm25(doc_id, postings):
    score = 0.0
    doc_len = doc_info[doc_id]["length"]
    for term, tf in postings:
        idf = idf_map.get(term, 0.0)
        denom = tf + k1 * (1 - b + b * (doc_len / avgdl))
        score += idf * ((tf * (k1 + 1)) / denom)
    return (doc_id, score)

# Group by doc_id: (doc_id, [(term, tf)])
rdd = sc.parallelize(inverted)
grouped = rdd.map(lambda x: (x[0], (x[1], x[2]))).groupByKey()
scores = grouped.map(lambda x: compute_bm25(x[0], list(x[1])))

# Show top 10 results
top_docs = scores.takeOrdered(10, key=lambda x: -x[1])

print("\n🔍 Top 10 Results for Query:", query_text)
print("=" * 50)
for rank, (doc_id, score) in enumerate(top_docs, 1):
    title = doc_info[doc_id]["title"]
    print(f"{rank:2d}. [Doc {doc_id}] {title} — Score: {score:.4f}")
