import os
import re

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from tqdm import tqdm


def clean_text(text):
    if text is None:
        return ""
    text = re.sub(r"\[\[.*?\|", "", text)
    text = re.sub(r"\[\[|\]\]", "", text)
    text = re.sub(r"<.*?>", "", text)
    text = re.sub(r"\{\{.*?\}\}", "", text)
    text = re.sub(r"\[.*?\]", "", text)
    text = re.sub(r"''+", "", text)
    text = re.sub(r"[\n\r\t]+", " ", text)
    return text.strip()


spark = (
    SparkSession.builder.appName("data preparation")
    .master("local")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .getOrCreate()
)

df = spark.read.parquet("/a.parquet")
df = df.select(["id", "title", "text"])

n = 1000
sampled_df = df.sample(fraction=100 * n / df.count(), seed=42).limit(n)

os.makedirs("data", exist_ok=True)
sampled_list = sampled_df.collect()

for row in tqdm(sampled_list, desc="Creating documents"):
    doc_id = str(row["id"])
    filename = f"{doc_id}_{sanitize_filename(row['title']).replace(' ', '_')}.txt"
    text = clean_text(row["text"])

    if text:
        with open(os.path.join("data", filename), "w", encoding="utf-8") as f:
            f.write(text)

sampled_df.rdd.map(
    lambda row: f"{row['id']}\t{sanitize_filename(row['title'])}\t{row['text']}"
).saveAsTextFile("/index/data")
