import random
import pika
import time

from tqdm import tqdm
import db

# Connect to RabbitMQ server
params = pika.ConnectionParameters(host="localhost", heartbeat=10)
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Declare queues
for shard_key in db.shard_keys:
    channel.queue_declare(queue=shard_key, durable=True)
    # channel.queue_purge(queue=shard_key)


def publish(rows, shard_key):
    # flush if data has more than 1000 points to keep memory in check
    channel.basic_publish(
        exchange='',
        routing_key=shard_key,
        body=",".join(rows),
        properties=pika.BasicProperties(
            delivery_mode = 2, # make message persistent
        )
    )


def add_followers(no_users=100, avg_user_follow_perc=0.01, hot_user_perc=0.005, hot_user_follow_perc=1):
    """
    Simulate adding followers to users in a social media platform.
    
    Parameters:
    - no_users (int): The number of total users in the platform (default: 100)
    - avg_user_follow_perc (float): The average percentage of users that a user follows (default: 0.01)
    - hot_user_perc (float): The percentage of users who are considered "hot" users and have a higher number of followers (default: 0.005)
    - hot_user_follow_perc (float): The percentage of users that a "hot" user follows (default: 1)
    
    Returns:
    - None
    
    Note:
    This function is only a simulation and doesn't actually add followers to any real users. 
    It can be used for testing and performance analysis of the sharding and pagination strategy for large number of followers.
    The values of these parameters should be set according to the distribution of the data in your system, and the query patterns. 
    The best values for one system may not be the best for another. It's recommended to test your system and monitor the performance of the queries with different values of these parameters and pick the best one.
    """
    hot_users = [random.randint(1, no_users) for _ in range(int(no_users * hot_user_perc / 100))]
    no_avg_user_followers = int(no_users * avg_user_follow_perc / 100)
    no_avg_user_followers = min(no_avg_user_followers, 500) # limit avg followers to 500
    no_hot_users = len(hot_users)
    no_hot_user_followers = int(no_users * hot_user_follow_perc / 100)
    no_hot_user_followers = min(no_hot_user_followers, 1000 * 100)
    print("no_users:", no_users)
    print("no_avg_user_followers:", no_avg_user_followers)
    print("no_hot_users:", no_hot_users)
    print("no_hot_user_followers:", no_hot_user_followers)
    print("no_records:", (((no_users-no_hot_users)*no_avg_user_followers*2) + (no_hot_users*no_hot_user_followers*2)))
    data = {shard_key: [] for shard_key in db.shard_keys}
    for source_id in tqdm(range(1, no_users +1), desc="Published..."):
        if source_id in hot_users:
            no_followers = no_hot_user_followers
        else:
            no_followers = no_avg_user_followers
        for _ in range(1, no_followers + 1):
            destination_id = random.randint(1, no_users)
            if source_id == destination_id:
                continue
            position = int(time.time() * 1000)
            # forward data (following)
            source_shard_key = db.get_shard_key(source_id)
            data[source_shard_key].append("({},{},{},{})".format(source_id, destination_id, position, 1))
            # backward data (followed by)
            destination_shard_key = db.get_shard_key(destination_id)
            data[destination_shard_key].append("({},{},{},{})".format(destination_id, source_id, position, 2))
            for shard_key, rows in data.items():
                if len(rows) > 1000:
                    publish(rows, shard_key)
                    data[shard_key] = []
    for shard_key, rows in data.items():
        publish(rows, shard_key)

if __name__ == '__main__':
    add_followers(no_users=500000)
    # close the connection
    connection.close()