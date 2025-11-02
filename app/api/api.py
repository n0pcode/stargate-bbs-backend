from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

WORKER_BASE_URL = "http://worker:5000"

# This helper function will forward the request and return the response
def proxy_request(method, path, **kwargs):
    """
    Forwards the request to the worker service and returns its response.
    """
    try:
        url = f"{WORKER_BASE_URL}{path}"
        
        response = requests.request(method, url, **kwargs)
        
        # Build a response to send back to the user
        return jsonify(response.json()), response.status_code
        
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Worker service is unavailable"}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# [C]REATE -> POST to worker
@app.route('/messages', methods=['POST'])
def create_message():
    return proxy_request('POST', '/messages', json=request.json)

# [R]EAD all -> GET from worker
@app.route('/messages', methods=['GET'])
def get_all_messages():
    return proxy_request('GET', '/messages')

# [R]EAD one -> GET from worker
@app.route('/messages/<int:message_id>', methods=['GET'])
def get_message(message_id):
    return proxy_request('GET', f'/messages/{message_id}')

# [U]PDATE -> PUT to worker
@app.route('/messages/<int:message_id>', methods=['PUT'])
def update_message(message_id):
    return proxy_request('PUT', f'/messages/{message_id}', json=request.json)

# [D]ELETE -> DELETE from worker
@app.route('/messages/<int:message_id>', methods=['DELETE'])
def delete_message(message_id):
    return proxy_request('DELETE', f'/messages/{message_id}')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)