from cassandra.cluster import Cluster

def create_schema():
    cluster = Cluster(["cassandra-server"])
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_index
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    session.set_keyspace("search_index")

    session.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            doc_id INT PRIMARY KEY,
            title TEXT,
            doc_length INT
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            term TEXT,
            doc_id INT,
            tf INT,
            PRIMARY KEY (term, doc_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary (
            term TEXT PRIMARY KEY,
            df INT,
            idf DOUBLE
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            key TEXT PRIMARY KEY,
            value DOUBLE
        )
    """)

    for table in ["documents", "inverted_index", "vocabulary", "stats"]:
        session.execute(f"TRUNCATE {table}")

    print("✅ Schema ready.")

if __name__ == "__main__":
    create_schema()
