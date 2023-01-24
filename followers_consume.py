import pika
import db
import multiprocessing


query_template = """
    INSERT INTO followers (source_id, destination_id, position, state)
    VALUES {}
    ON CONFLICT (source_id, destination_id, position, state)
    DO NOTHING;
"""

def callback(ch, method, properties, body):
    """
    callback function to handle received messages
    """
    print("recieved message", method.delivery_tag, "on queue", method.routing_key)
    data = str(body, "utf-8")
    shard_key = method.routing_key
    query = query_template.format(data)
    db.query_shard(query, shard_key, commit=True)
    # insert_to_db(data, shard_key)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def consume_messages(queue_name):
    """
    consume messages
    """
    params = pika.ConnectionParameters(host="localhost", heartbeat=10)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.basic_qos(prefetch_count=10)
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=False
    )
    channel.start_consuming()
    connection.close()

if __name__ == '__main__':
    shard_keys = db.shard_keys
    with multiprocessing.Pool(processes=len(shard_keys)) as pool:
        pool.map(consume_messages, shard_keys)
