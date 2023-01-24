import db


def create_follower_table():
    """
    create a followers table in all shards
    """
    query = """
        CREATE TABLE IF NOT EXISTS followers (
            source_id BIGINT,
            destination_id BIGINT,
            position BIGINT,
            state SMALLINT,
            PRIMARY KEY (source_id, destination_id, state, position)
        );
    """
    db.query_all_shards(query, commit=True)


def drop_follower_table():
    """
    delete followers tables in all shards
    """
    query = "DROP TABLE IF EXISTS followers;"
    db.query_all_shards(query, commit=True)


def count_rows():
    query = """
        SELECT COUNT(1) FROM followers;
    """
    db.query_all_shards(query, fetch=True, print_=True)


def get_hot_users():
    query = """
        SELECT source_id, COUNT(destination_id)
        FROM followers
        GROUP BY source_id
        ORDER BY COUNT(destination_id) DESC
        LIMIT 5;
    """
    db.query_all_shards(query, fetch=True, print_=True)


def get_followers(source_id):
    shard_key = db.get_shard_key(source_id)
    query = f"""
        SELECT * FROM followers
        WHERE
            source_id = {source_id}
            AND state = 1
        LIMIT 100;
    """
    db.query_shard(query=query, shard_key=shard_key, fetch=True, print_=True)



if __name__ == '__main__':
    # drop_follower_table()
    # create_follower_table()
    # count_rows()
    # get_hot_users()
    get_followers(source_id=117240)
    pass

