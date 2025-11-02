from flask import Flask, request, jsonify
import redis
import time

app = Flask(__name__)

# Connect to the Redis message DB
try:
    redis_db = redis.from_url('redis://redis:6379/1', decode_responses=True)
    redis_db.ping()
    print("WORKER: Connected to Redis DB 1 successfully.")
except Exception as e:
    print(f"WORKER: Failed to connect to Redis DB 1: {e}")

# --- Worker's Internal Job Endpoints ---

def set_job_status(job_id, status):
    """Helper function to update job status in Redis."""
    redis_db.set(f"job:{job_id}", status)

# [C]REATE a new message
@app.route('/create_message', methods=['POST'])
def create_message():
    data = request.json
    content = data.get('content')
    job_id = data.get('job_id')
    
    if not content or not job_id:
        return jsonify({"error": "Missing 'content' or 'job_id'"}), 400
    
    try:
        new_id = redis_db.incr('message_counter')
        key = f'message:{new_id}'
        message = {"id": new_id, "content": content}
        redis_db.hset(key, mapping=message)
        
        time.sleep(1) # Simulate a long-running job
        
        set_job_status(job_id, "completed")
        print(f"WORKER: Completed job {job_id} (Create message {new_id})")
        return jsonify(message), 201
        
    except Exception as e:
        set_job_status(job_id, f"failed: {e}")
        return jsonify({"error": str(e)}), 500

# [U]PDATE a message
@app.route('/update_message/<int:message_id>', methods=['PUT'])
def update_message(message_id):
    data = request.json
    content = data.get('content')
    job_id = data.get('job_id')
    key = f'message:{message_id}'

    if not redis_db.exists(key):
        set_job_status(job_id, "failed: not_found")
        return jsonify({"error": "Message not found"}), 404
    
    try:
        redis_db.hset(key, 'content', content)
        data = redis_db.hgetall(key)
        
        time.sleep(1) # Simulate work
        
        set_job_status(job_id, "completed")
        print(f"WORKER: Completed job {job_id} (Update message {message_id})")
        return jsonify(data)
    except Exception as e:
        set_job_status(job_id, f"failed: {e}")
        return jsonify({"error": str(e)}), 500

# [D]ELETE a message
@app.route('/delete_message/<int:message_id>', methods=['DELETE'])
def delete_message(message_id):
    data = request.json
    job_id = data.get('job_id')
    key = f'message:{message_id}'
    
    result = redis_db.delete(key)
    if result == 0:
        set_job_status(job_id, "failed: not_found")
        return jsonify({"error": "Message not found"}), 404
        
    time.sleep(1) # Simulate work
    
    set_job_status(job_id, "completed")
    print(f"WORKER: Completed job {job_id} (Delete message {message_id})")
    return jsonify({"status": "deleted", "id": message_id}), 200