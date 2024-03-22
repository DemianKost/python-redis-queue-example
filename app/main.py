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