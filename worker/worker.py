"""
Worker app

Here we will process all messages that we will get from the Redis queue
"""

import redis
import config

import random
from json import loads

def redis_db():
    db = redis.Redis(
        host=config.redis_host,
        port=config.redis_port,
        db=config.redis_db_number,
        decode_responses=True
    )

    # Check that redis is running
    db.ping()

    return db

# We will use this message to simulate when queue fails something
def redis_queue_push(db, message):
    # push to tail of the queue
    db.lpush(config.redis_queue_name, message)

def redis_queue_pop(db):
    # pop from head of the queue (right of the list)
    # the `b` in `brpop` indicates this is a blocking call (wait until items become available)
    _, message_json = db.brpop(config.redis_queue_name)
    return message_json

def process_message(db, message_json: str):
    message = loads(message)
    print(f"Message received: id={message['id']}, message_number={message['data']['message_number']}")

    # mimic potential processing errors
    processed_ok = random.choices((True, False), weights=(5, 1), k=1)[0]
    if processed_ok:
        print("Processed successfully")
    else:
        print(f"\tProcessing failed - requeuening...")
        redis_queue_push(db, message_json)

def main():
    """
    Consumes items from the queue
    """
    # Connect to Redis db
    db = redis_db()

    while True:
        message_json = redis_queue_pop(db)
        process_message(db, message_json)

if __name__ == '__main__':
    main()