#!/usr/bin/env python3
import re
import sys
from collections import defaultdict

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

for line in sys.stdin:
    parts = line.strip().split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id_str, raw_title, raw_text = parts
    doc_id = int(doc_id_str)
    title = raw_title.replace("_", " ")
    full_text = f"{title} {raw_text}"
    tokens = tokenize(full_text)

    term_counts = defaultdict(int)
    for tok in tokens:
        term_counts[tok] += 1

    print(f"__META__\t{doc_id}\t{title}")
    print(f"__DOCLEN__\t{doc_id}\t{len(tokens)}")

    for term, tf in term_counts.items():
        print(f"{term}\t{doc_id}\t{tf}")
