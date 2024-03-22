"""
Main app - where all messages are pushed to the Redis queue
"""
import random
from datetime import datetime
from json import dumps
from time import sleep
from uuid import uuid4

import redis
import config

def redis_db():
    db = redis.Redis(
        host=config.redis_host,
        port=config.redis_port,
        db=config.redis_db_number,
        password=config.redis_password,
        decode_responses=True
    )

    # Check that redis is running
    db.ping()

    return db

def redis_queue_push(db, message):
    # push to tail of the queue
    db.lpush(config.redis_queue_name, message)

def main(num_messages: int, delay: float = 1):
    """
    Generate num of messages and push it to the Redis queue
    :param num_messages:
    :return:
    """

    # Connect to Redis db
    db = redis_db()

    for i in range(num_messages):
        # Setup message data
        message = {
            "id": str(uuid4()),
            "ts": datetime.utcnow().isoformat(),
            "data": {
                "message_number": i,
                "x": random.randrange(0, 100),
                "y": random.randrange(0, 100),
            }
        }

        # All message data we will store as JSON in Redis DB
        message_json = dumps(message)

        # Push message to queue
        print(f"Sending message {i} (id={message['id']})")
        redis_queue_push(db, message_json)

        # Wait a little bit
        sleep(delay)

if __name__ == "__main__":
    main(30, 0.1)