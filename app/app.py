from cassandra.cluster import Cluster

s = Cluster(["cassandra-server"]).connect()
s.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_index
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
""")
s.set_keyspace("search_index")

s.execute("DROP TABLE IF EXISTS stats")
s.execute("""
    CREATE TABLE stats (
        key TEXT PRIMARY KEY,
        value DOUBLE
    )
""")

s.execute("DROP TABLE IF EXISTS inverted_index")
s.execute("""
    CREATE TABLE inverted_index (
        term TEXT,
        doc_id INT,
        tf INT,
        PRIMARY KEY (term, doc_id)
    )
""")

s.execute("DROP TABLE IF EXISTS vocabulary")
s.execute("""
    CREATE TABLE vocabulary (
        term TEXT PRIMARY KEY,
        df INT,
        idf DOUBLE
    )
""")

s.execute("DROP TABLE IF EXISTS documents")
s.execute("""
    CREATE TABLE documents (
        doc_id INT PRIMARY KEY,
        doc_length INT,
        title TEXT
    )
""")
