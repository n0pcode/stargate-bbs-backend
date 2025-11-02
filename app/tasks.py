import redis
from celery import Celery

# --- Redis Clients ---

# NOTE: We use different 'db' numbers to separate our data.
# db=0 will be the Celery message broker
# db=1 will be our application's message database

CELERY_BROKER_URL = 'redis://redis:6379/0'
MESSAGE_DB_URL = 'redis://redis:6379/1'


# Redis client for our application data (the messages)
# decode_responses=True makes it return strings instead of bytes
redis_db = redis.from_url(MESSAGE_DB_URL, decode_responses=True)

# --- Celery App Definition ---
celery_app = Celery('tasks', broker=CELERY_BROKER_URL)


# --- Worker Tasks (The C-U-D logic) ---

@celery_app.task(name='tasks.create_message')
def create_message(content):
    """
    Creates a new message post.
    We use a Redis counter to get a unique, incrementing ID.
    """
    # INCR returns the *new* value of the counter, giving us a unique ID
    new_id = redis_db.incr('message_counter')
    
    # We store the message in a Redis Hash
    # KEY: "message:1", FIELD: "content", VALUE: "..."
    redis_db.hset(f'message:{new_id}', mapping={'content': content})
    
    print(f"WORKER: Created message {new_id}: {content}")
    return new_id

@celery_app.task(name='tasks.update_message')
def update_message(message_id, content):
    """
    Updates the content of an existing message.
    """
    if redis_db.exists(f'message:{message_id}'):
        redis_db.hset(f'message:{message_id}', 'content', content)
        print(f"WORKER: Updated message {message_id}: {content}")
        return True
    print(f"WORKER: FAILED to update message {message_id}. Not found.")
    return False

@celery_app.task(name='tasks.delete_message')
def delete_message(message_id):
    """
    Deletes a message hash from Redis.
    """
    result = redis_db.delete(f'message:{message_id}')
    if result > 0:
        print(f"WORKER: Deleted message {message_id}")
        return True
    print(f"WORKER: FAILED to delete message {message_id}. Not found.")
    return False