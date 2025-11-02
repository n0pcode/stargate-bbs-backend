from flask import Flask, request, jsonify
import requests
import redis  # <-- Import redis
import uuid   # <-- To create unique job IDs

app = Flask(__name__)

# --- Redis Client (for READS and JOB STATUS) ---
try:
    # We will use DB 1 for both messages and job statuses
    redis_db = redis.from_url('redis://redis:6379/1', decode_responses=True)
    redis_db.ping()
    print("API: Connected to Redis DB 1 successfully.")
except Exception as e:
    print(f"API: Failed to connect to Redis DB 1: {e}")


# The base URL for the worker service (for WRITE jobs)
WORKER_BASE_URL = "http://worker:5000"


def proxy_job_request(method, path, **kwargs):
    """
    Forwards a C-U-D job to the worker service.
    This is "fire and forget" - we don't wait for the worker,
    we just need to send it the job.
    """
    try:
        url = f"{WORKER_BASE_URL}{path}"
        # We use a short timeout. We don't wait for the job
        # to finish, only for the worker to accept it.
        requests.request(method, url, timeout=3, **kwargs)
    except requests.exceptions.Timeout:
        # Worker is busy, but it will eventually process
        print(f"API: Worker call for {path} timed out (this is OK).")
    except requests.exceptions.ConnectionError:
        print(f"API: Worker service is unavailable for {path}.")
        # In a real app, you'd mark the job as 'failed' here
    except Exception as e:
        print(f"API: Error proxying job {path}: {e}")

# --- API Endpoints ---

# [C]REATE Job
@app.route('/messages', methods=['POST'])
def create_message():
    content = request.json.get('content')
    if not content:
        return jsonify({"error": "Missing 'content'"}), 400
    
    # 1. Create a Job ID
    job_id = str(uuid.uuid4())
    job_key = f"job:{job_id}"
    
    # 2. Write job status to Redis
    redis_db.set(job_key, "pending")
    
    # 3. Call worker by service name
    job_payload = {"content": content, "job_id": job_id}
    proxy_job_request('POST', '/create_message', json=job_payload)
    
    # 4. Return the Job ID immediately
    return jsonify({"job_id": job_id, "status_url": f"/jobs/{job_id}/status"}), 202

# [U]PDATE Job
@app.route('/messages/<int:message_id>', methods=['PUT'])
def update_message(message_id):
    content = request.json.get('content')
    if not content:
        return jsonify({"error": "Missing 'content'"}), 400
    
    job_id = str(uuid.uuid4())
    job_key = f"job:{job_id}"
    redis_db.set(job_key, "pending")
    
    job_payload = {"content": content, "job_id": job_id}
    proxy_job_request('PUT', f'/update_message/{message_id}', json=job_payload)
    
    return jsonify({"job_id": job_id, "status_url": f"/jobs/{job_id}/status"}), 202

# [D]ELETE Job
@app.route('/messages/<int:message_id>', methods=['DELETE'])
def delete_message(message_id):
    job_id = str(uuid.uuid4())
    job_key = f"job:{job_id}"
    redis_db.set(job_key, "pending")
    
    job_payload = {"job_id": job_id}
    proxy_job_request('DELETE', f'/delete_message/{message_id}', json=job_payload)
    
    return jsonify({"job_id": job_id, "status_url": f"/jobs/{job_id}/status"}), 202

# --- Direct Read Endpoints (Data) ---

@app.route('/messages', methods=['GET'])
def get_all_messages():
    messages = {}
    for key in redis_db.scan_iter("message:*"):
        data = redis_db.hgetall(key)
        if data:
            messages[data['id']] = data['content']
    return jsonify(messages)

@app.route('/messages/<int:message_id>', methods=['GET'])
def get_message(message_id):
    data = redis_db.hgetall(f'message:{message_id}')
    if not data:
        return jsonify({"error": "Message not found"}), 404
    return jsonify(data)

# --- Direct Read Endpoint (Job Status) ---

@app.route('/jobs/<string:job_id>/status', methods=['GET'])
def get_job_status(job_id):
    """
    This is the new endpoint. It reads the job status
    DIRECTLY from Redis.
    """
    status = redis_db.get(f"job:{job_id}")
    if not status:
        return jsonify({"error": "Job not found"}), 404
    
    return jsonify({"job_id": job_id, "status": status})