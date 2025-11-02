from flask import Flask, request, jsonify
import redis

app = Flask(__name__)

try:
    redis_db = redis.from_url('redis://redis:6379/1', decode_responses=True)
    redis_db.ping()
    print("WORKER: Connected to Redis DB 1 successfully.")
except Exception as e:
    print(f"WORKER: Failed to connect to Redis DB 1: {e}")

# [C]REATE a new message
@app.route('/messages', methods=['POST'])
def create_message():
    content = request.json.get('content')
    if not content:
        return jsonify({"error": "Missing 'content'"}), 400
    
    # Get a new unique ID
    new_id = redis_db.incr('message_counter')
    key = f'message:{new_id}'
    
    # Store the message as a Redis Hash
    message = {"id": new_id, "content": content}
    redis_db.hset(key, mapping=message)
    
    print(f"WORKER: Created {key}")
    return jsonify(message), 201

# [R]EAD all messages
@app.route('/messages', methods=['GET'])
def get_all_messages():
    messages = {}
    for key in redis_db.scan_iter("message:*"):
        data = redis_db.hgetall(key)
        messages[data['id']] = data['content']
    print(f"WORKER: Read all {len(messages)} messages")
    return jsonify(messages)

# [R]EAD one message
@app.route('/messages/<int:message_id>', methods=['GET'])
def get_message(message_id):
    key = f'message:{message_id}'
    data = redis_db.hgetall(key)
    if not data:
        return jsonify({"error": "Message not found"}), 404
    print(f"WORKER: Read {key}")
    return jsonify(data)

# [U]PDATE a message
@app.route('/messages/<int:message_id>', methods=['PUT'])
def update_message(message_id):
    key = f'message:{message_id}'
    if not redis_db.exists(key):
        return jsonify({"error": "Message not found"}), 404
        
    content = request.json.get('content')
    if not content:
        return jsonify({"error": "Missing 'content'"}), 400
    
    redis_db.hset(key, 'content', content)
    data = redis_db.hgetall(key)
    print(f"WORKER: Updated {key}")
    return jsonify(data)

# [D]ELETE a message
@app.route('/messages/<int:message_id>', methods=['DELETE'])
def delete_message(message_id):
    key = f'message:{message_id}'
    if redis_db.delete(key) == 0:
        return jsonify({"error": "Message not found"}), 404
    
    print(f"WORKER: Deleted {key}")
    return jsonify({"status": "deleted", "id": message_id}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)