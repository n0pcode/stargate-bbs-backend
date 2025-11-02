import pytest
from api import app

# --- Fixtures ---

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

@pytest.fixture
def mock_redis(mocker):
    """
    Mock the 'redis_db' object *directly* in the 'api' module.
    """
    # Create a mock object that will pretend to be our redis_db
    mock_db = mocker.Mock()
    
    # Patch the 'redis_db' variable *inside the api module*
    # This replaces the real object with our mock.
    mocker.patch('api.redis_db', new=mock_db)
    
    return mock_db

@pytest.fixture
def mock_requests(mocker):
    # Patch the 'request' method inside the 'requests' library
    mock_post = mocker.patch('requests.request')
    return mock_post

# --- Test Cases ---

def test_get_messages(client, mock_redis):
    """
    Test GET /messages
    It should call redis_db.scan_iter and redis_db.hgetall
    """
    # Define what our mock_redis should return
    mock_redis.scan_iter.return_value = ['message:1']
    mock_redis.hgetall.return_value = {'id': '1', 'content': 'Test Message'}

    # Make the test request
    response = client.get('/messages')

    # Assertions
    assert response.status_code == 200
    assert response.json == {'1': 'Test Message'}
    mock_redis.scan_iter.assert_called_with('message:*')
    mock_redis.hgetall.assert_called_with('message:1')

def test_get_one_message(client, mock_redis):
    """
    Test GET /messages/<id>
    It should call redis_db.hgetall with the correct key
    """
    mock_redis.hgetall.return_value = {'id': '1', 'content': 'Test Message'}
    
    response = client.get('/messages/1')
    
    assert response.status_code == 200
    assert response.json == {'id': '1', 'content': 'Test Message'}
    mock_redis.hgetall.assert_called_with('message:1')

def test_get_one_message_404(client, mock_redis):
    """
    Test GET /messages/<id> when the message is not found
    """
    mock_redis.hgetall.return_value = None  # Simulate message not found
    
    response = client.get('/messages/99')
    
    assert response.status_code == 404
    assert response.json == {'error': 'Message not found'}

def test_get_job_status(client, mock_redis):
    """
    Test GET /jobs/<job_id>/status
    It should call redis_db.get with the correct job key
    """
    mock_redis.get.return_value = "completed"
    
    response = client.get('/jobs/test-job-id/status')
    
    assert response.status_code == 200
    assert response.json == {'job_id': 'test-job-id', 'status': 'completed'}
    mock_redis.get.assert_called_with('job:test-job-id')

def test_get_job_status_404(client, mock_redis):
    """
    Test GET /jobs/<job_id>/status when the job is not found
    """
    mock_redis.get.return_value = None  # Simulate job not found
    
    response = client.get('/jobs/fake-id/status')
    
    assert response.status_code == 404
    assert response.json == {'error': 'Job not found'}

def test_post_message(client, mock_redis, mock_requests):
    """
    Test POST /messages
    It should:
    1. Set job status in Redis
    2. Call the worker via requests
    3. Return a 202 with a job_id
    """
    response = client.post('/messages', json={'content': 'New Message'})
    
    # Check the response
    assert response.status_code == 202
    job_id = response.json['job_id']
    assert response.json['status_url'] == f'/jobs/{job_id}/status'
    
    # Check that Redis was called to set "pending" status
    mock_redis.set.assert_called_with(f'job:{job_id}', 'pending')
    
    # Check that the worker was called
    expected_url = 'http://worker:5000/create_message'
    expected_payload = {'content': 'New Message', 'job_id': job_id}
    mock_requests.assert_called_with(
        'POST',
        expected_url,
        timeout=3,
        json=expected_payload
    )

def test_put_message(client, mock_redis, mock_requests):
    """Test PUT /messages/<id>"""
    response = client.put('/messages/1', json={'content': 'Updated'})
    
    assert response.status_code == 202
    job_id = response.json['job_id']
    
    mock_redis.set.assert_called_with(f'job:{job_id}', 'pending')
    
    expected_url = 'http://worker:5000/update_message/1'
    expected_payload = {'content': 'Updated', 'job_id': job_id}
    mock_requests.assert_called_with(
        'PUT',
        expected_url,
        timeout=3,
        json=expected_payload
    )

def test_delete_message(client, mock_redis, mock_requests):
    """Test DELETE /messages/<id>"""
    response = client.delete('/messages/1')
    
    assert response.status_code == 202
    job_id = response.json['job_id']
    
    mock_redis.set.assert_called_with(f'job:{job_id}', 'pending')
    
    expected_url = 'http://worker:5000/delete_message/1'
    expected_payload = {'job_id': job_id}
    mock_requests.assert_called_with(
        'DELETE',
        expected_url,
        timeout=3,
        json=expected_payload
    )