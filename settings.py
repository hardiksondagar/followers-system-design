no_shards = 10
base_path = "/Users/hardiksondagar/Work/data"
shard_connections = {f"shard{i}": {"port": f"5432{i}"} for i in range(no_shards)}
