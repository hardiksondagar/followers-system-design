import time
import psycopg2
from psycopg2.pool import SimpleConnectionPool




# Define a dictionary of shard connections
no_shards = 10
base_path = "~/Work/system-design-cohort/data/pg_shards"
shard_connections = {
    f"shard{i}": {"host": "localhost", "port": f"5432{i}"} for i in range(no_shards)
}

# Define a list of shard keys using shard_connections key
shard_keys = list(shard_connections.keys())

def timer(func):
    """
    python decorator to measure functino running time
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f'{func.__name__} executed in {end_time - start_time} seconds.')
        return result
    return wrapper


def get_shard_connection(shard_key):
    """
    function to get a connection from a specific shard's connection pool
    """
    conn_params = shard_connections[shard_key]
    conn = psycopg2.connect(
        host=conn_params.get("host", "localhost"),
        port=conn_params.get("port", 5432),
        user=conn_params.get("user", "postgres"),
        password=conn_params.get("password", "postgres"),
        dbname=conn_params.get("dbname", "postgres"),
    )
    return conn

connection_pools = {}
for shard_key, conn_params in shard_connections.items():
    connection_pools[shard_key] = SimpleConnectionPool(
        minconn=2,
        maxconn=5,
        host=conn_params.get("host", "localhost"),
        port=conn_params.get("port", 5432),
        user=conn_params.get("user", "postgres"),
        password=conn_params.get("password", "postgres"),
        dbname=conn_params.get("dbname", "postgres")
    )

def get_shard_connection_from_pool(shard_key):
    pass


@timer
def query_all_shards(query, commit=False, fetch=False, print_=False):
    """
    run query in all shards
    """
    results = {}
    for shard_key in shard_keys:
        conn = get_shard_connection(shard_key)
        cur = conn.cursor()
        cur.execute(query)
        if commit:
            conn.commit()
        if fetch:
            rows = cur.fetchall()
            results[shard_key] = rows
            if print_:
                print(shard_key)
                for row in rows:
                    print(row)
    return results


@timer
def query_shard(query, shard_key, commit=False, fetch=False, print_=False):
    """
    run query on given shard
    """
    try:
        conn = get_shard_connection(shard_key)
        cur = conn.cursor()
        cur.execute(query)
        if fetch:
            rows = cur.fetchall()
            result = rows
            if print_:
                for row in rows:
                    print(row)
        else:
            result = None
        if commit:
            conn.commit()
        cur.close()
        conn.close()
        return result
    except Exception as e:
        cur.close()
        conn.close()
        print(e)
        return None

def get_shard_key(user_id):
    """
    function to determine the shard key based on the user_id
    """
    return f"shard{user_id % no_shards}"