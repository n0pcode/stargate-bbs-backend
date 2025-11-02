from flask import Flask, request, jsonify
from tasks import celery_app, redis_db  # Import from our tasks.py
import tasks  # Import the tasks module itself to call them

app = Flask(__name__)

# --- API Endpoints ---

# [C]REATE -> Queues a 'create' task
@app.route('/messages', methods=['POST'])
def post_message():
    content = request.json.get('content')
    if not content:
        return jsonify({"error": "Missing 'content' in JSON body"}), 400
    
    # Send the task to the Celery queue
    # The worker will pick this up
    tasks.create_message.delay(content)
    
    # Return 202 Accepted, as the job is queued but not yet complete
    return jsonify({"status": "Message creation queued"}), 202

# [R]EAD (All) -> Reads *directly* from Redis
@app.route('/messages', methods=['GET'])
def get_all_messages():
    messages = {}
    # Scan for all keys matching the "message:*" pattern
    for key in redis_db.scan_iter("message:*"):
        # Get the ID from the key name (e.g., "message:1" -> "1")
        message_id = key.split(':')[-1]
        # Get the content from the hash
        content = redis_db.hget(key, 'content')
        messages[message_id] = content
        
    return jsonify(messages)

# [R]EAD (One) -> Reads *directly* from Redis
@app.route('/messages/<int:message_id>', methods=['GET'])
def get_message(message_id):
    content = redis_db.hget(f'message:{message_id}', 'content')
    if not content:
        return jsonify({"error": "Message not found"}), 404
    
    return jsonify({"id": message_id, "content": content})

# [U]PDATE -> Queues an 'update' task
@app.route('/messages/<int:message_id>', methods=['PUT'])
def update_message(message_id):
    content = request.json.get('content')
    if not content:
        return jsonify({"error": "Missing 'content' in JSON body"}), 400
    
    # Send the update task to the queue
    tasks.update_message.delay(message_id, content)
    return jsonify({"status": f"Message {message_id} update queued"}), 202

# [D]ELETE -> Queues a 'delete' task
@app.route('/messages/<int:message_id>', methods=['DELETE'])
def delete_message(message_id):
    # Send the delete task to the queue
    tasks.delete_message.delay(message_id)
    return jsonify({"status": f"Message {message_id} deletion queued"}), 202