# Web Server Test Coverage Documentation

This document provides a comprehensive overview of the unit tests for the web-server FastAPI application.

## Test Statistics

- **Total Tests**: 39
- **Test File**: `/home/dotan/projects/unityProj/apps/web-server/test_app.py`
- **All Tests**: PASSING ✓

## Running Tests

```bash
# Run all tests
cd apps/web-server
pytest test_app.py -v

# Run tests with coverage (requires pytest-cov)
pytest test_app.py --cov=app --cov-report=html

# Run specific test
pytest test_app.py::test_create_purchase_success -v

# Run tests matching a pattern
pytest test_app.py -k "lifespan" -v
```

## Test Categories

### 1. Health Check Tests (1 test)

#### `test_health_endpoint()`
- Verifies `/health` endpoint returns 200 OK
- Ensures health check is independent of dependencies

#### `test_health_check_independent_of_dependencies()`
- Verifies health check works even when Kafka producer is None
- Tests resilience of health endpoint

---

### 2. Lifespan Management Tests (4 tests)

These tests verify proper startup and shutdown behavior of the application.

#### `test_lifespan_kafka_producer_initialization()`
- Verifies Kafka producer is initialized on startup
- Confirms producer.start() is called
- Ensures producer is set to global variable
- Verifies producer.stop() is called on shutdown

#### `test_lifespan_producer_configuration()`
- Validates producer configuration parameters:
  - `acks='all'` for strongest delivery guarantee
  - `request_timeout_ms=30000` (30 seconds)
  - `value_serializer` function is present

#### `test_lifespan_graceful_shutdown()`
- Tests graceful shutdown behavior
- Ensures producer.stop() is called to flush pending messages

#### `test_lifespan_value_serializer_json()`
- Tests JSON serialization function
- Verifies data is correctly converted to bytes
- Ensures JSON structure is preserved

---

### 3. Purchase Creation Tests - /buy endpoint (12 tests)

#### Success Scenarios

##### `test_create_purchase_success()`
- Tests successful purchase creation
- Verifies 200 response with success message
- Confirms Kafka producer is called

##### `test_create_purchase_message_serialization()`
- Validates all fields are included in Kafka message
- Confirms field values are preserved
- Verifies timestamp is added

##### `test_create_purchase_partition_key_encoding()`
- Tests user_id is correctly encoded as bytes for partition key
- Ensures proper UTF-8 encoding: `user_id.encode('utf-8')`

##### `test_create_purchase_timestamp_generation()`
- Validates server-side timestamp generation
- Uses mocked datetime to verify ISO format timestamp

##### `test_create_purchase_kafka_topic_name()`
- Confirms messages are sent to correct topic ("purchases")

#### Error Scenarios

##### `test_create_purchase_producer_none()`
- Tests behavior when Kafka producer is not initialized
- Expects 503 Service Unavailable
- Validates error message

##### `test_create_purchase_kafka_send_failure()`
- Simulates Kafka send failure
- Verifies 500 error with descriptive message
- Tests error propagation from Kafka

#### Validation Tests

##### `test_create_purchase_invalid_data()`
- Tests request with missing required fields
- Expects 422 Validation Error

##### `test_create_purchase_invalid_price()`
- Tests invalid price type (string instead of float)
- Expects 422 Validation Error

---

### 4. Purchase Retrieval Tests - /purchases/{user_id} (9 tests)

#### Success Scenarios

##### `test_get_purchases_success()`
- Tests successful retrieval of user purchases
- Validates response structure
- Confirms data is returned correctly

##### `test_get_purchases_with_pagination()`
- Tests pagination parameter passing
- Verifies limit and offset are sent to Management API

##### `test_get_purchases_pagination_default_values()`
- Confirms default values: limit=100, offset=0

##### `test_get_purchases_pagination_custom_values()`
- Tests custom pagination values are passed correctly

##### `test_get_purchases_timeout_configuration()`
- Verifies 10 second timeout is set for Management API calls

##### `test_get_purchases_special_characters_in_user_id()`
- Tests URL encoding handles special characters
- Example: `/purchases/user@123!test`

#### Error Scenarios

##### `test_get_purchases_timeout_handling()`
- Tests timeout exception from httpx
- Expects 504 Gateway Timeout
- Validates error message

##### `test_get_purchases_404_error()`
- Tests 404 Not Found from Management API
- Verifies status code propagation

##### `test_get_purchases_500_error()`
- Tests 500 Internal Server Error from Management API
- Confirms error propagation

##### `test_get_purchases_network_failure()`
- Simulates network connection failure (httpx.ConnectError)
- Expects 500 error

##### `test_get_purchases_response_parsing_error()`
- Tests JSON parsing errors
- Simulates invalid JSON response
- Expects 500 error

---

### 5. Edge Cases Tests (9 tests)

These tests validate system behavior with unusual but valid inputs.

#### String Field Tests

##### `test_create_purchase_empty_username()`
- Tests empty string in username field
- Documents current behavior (accepts empty strings)

##### `test_create_purchase_whitespace_fields()`
- Tests whitespace-only strings
- Confirms system handles whitespace

##### `test_create_purchase_very_long_strings()`
- Tests 1000-character strings in all fields
- Verifies no truncation occurs
- Confirms data integrity

##### `test_create_purchase_special_characters_in_fields()`
- Tests special characters: `<script>`, `&`, quotes, etc.
- Validates XSS-like patterns are preserved (not sanitized)

##### `test_create_purchase_unicode_characters()`
- Tests Unicode characters: Chinese, Arabic, Japanese
- Example: "魔法のポーション"
- Confirms UTF-8 encoding works correctly

#### Price Field Tests

##### `test_create_purchase_negative_price()`
- Tests negative price values (-9.99)
- Documents current behavior (accepts negative prices)
- Suggests business logic validation consideration

##### `test_create_purchase_zero_price()`
- Tests zero price (free items)
- Confirms 0.0 is preserved correctly

##### `test_create_purchase_very_large_price()`
- Tests very large prices (999999999.99)
- Verifies no overflow or precision loss

---

### 6. Integration Scenarios Tests (4 tests)

These tests verify complete flows with all components working together.

#### `test_full_purchase_flow_success()`
- End-to-end test of purchase creation
- Validates:
  - HTTP request/response
  - Kafka producer interaction
  - Message structure and content
  - Partition key assignment
  - Timestamp generation

#### `test_full_retrieval_flow_success()`
- End-to-end test of purchase retrieval
- Validates:
  - HTTP request to Management API
  - Query parameter passing
  - Response parsing
  - Timeout configuration

#### `test_error_propagation_from_kafka()`
- Tests error flow from Kafka to client
- Simulates broker unavailability
- Verifies error message includes root cause

#### `test_error_propagation_from_management_api()`
- Tests error flow from Management API to client
- Verifies HTTP status code propagation (503)

#### `test_concurrent_purchase_requests()`
- Tests handling of 5 concurrent purchase requests
- Verifies all requests succeed
- Confirms Kafka producer is called for each request

---

## Test Fixtures

### `mock_producer`
- Creates a mock Kafka producer with AsyncMock methods
- Methods: `send_and_wait`, `start`, `stop`

### `sample_purchase_data`
- Provides standard valid purchase data
- Fields: username, user_id, item_name, price

### `sample_purchases_response`
- Mock response from Management API
- Includes array of purchases with metadata

### `mock_httpx_client`
- Creates a mock httpx AsyncClient
- Properly mocks async context manager

---

## Mocking Strategy

### External Dependencies Mocked

1. **aiokafka.AIOKafkaProducer**
   - Mocked at module level before import
   - Individual tests mock at function level for specific behavior

2. **httpx.AsyncClient**
   - Mocked using `@patch('app.httpx.AsyncClient')`
   - Properly handles async context manager (`__aenter__`, `__aexit__`)

3. **datetime.utcnow()**
   - Mocked for timestamp consistency testing
   - Uses fixed datetime for deterministic tests

### Mocking Patterns Used

```python
# Pattern 1: Patching at function level
@patch('app.AIOKafkaProducer')
def test_function(mock_producer_class):
    ...

# Pattern 2: Direct module manipulation
import app as app_module
original_producer = app_module.producer
app_module.producer = mock_producer
try:
    # Test code
finally:
    app_module.producer = original_producer

# Pattern 3: AsyncMock for async methods
mock_client = AsyncMock()
mock_client.get.return_value = mock_response
```

---

## Coverage Analysis

### Well-Covered Areas
- ✓ All HTTP endpoints (health, buy, purchases)
- ✓ Lifespan management (startup/shutdown)
- ✓ Error handling (timeouts, network errors, HTTP errors)
- ✓ Kafka integration (send, partition keys, serialization)
- ✓ Input validation (missing fields, invalid types)
- ✓ Edge cases (empty strings, large values, special characters)
- ✓ Pagination (default and custom values)

### Areas for Consideration

1. **Business Logic Validation**
   - Current tests document that negative prices are accepted
   - Empty strings in required fields are accepted
   - Consider adding validation if business rules require it

2. **Concurrent Access**
   - Tests verify sequential concurrent requests
   - Could add tests for race conditions with shared resources

3. **Retry Logic**
   - Kafka producer has retry configuration
   - Could add tests for retry behavior

4. **Performance Testing**
   - Current tests are functional
   - Load testing is separate (see `test/load-test.py`)

---

## Test Dependencies

Required packages (from `requirements.txt`):
```
fastapi==0.109.0
uvicorn==0.27.0
aiokafka==0.10.0
httpx==0.26.0
pydantic==2.5.3
pytest==7.4.3
pytest-asyncio==0.21.1
```

Optional for coverage:
```
pytest-cov
```

---

## Pytest Configuration

Configuration in `/home/dotan/projects/unityProj/apps/web-server/pytest.ini`:

```ini
[pytest]
asyncio_mode = auto           # Enable async test support
python_files = test_*.py      # Test file pattern
python_functions = test_*     # Test function pattern
addopts = -v --tb=short       # Verbose output, short traceback
```

---

## Best Practices Demonstrated

1. **Arrange-Act-Assert Pattern**
   - All tests follow clear structure
   - Setup → Execute → Verify

2. **Fixture Reuse**
   - Common mock objects in fixtures
   - Reduces code duplication

3. **Descriptive Test Names**
   - Format: `test_<function>_<scenario>_<expected>`
   - Examples: `test_create_purchase_kafka_send_failure`

4. **Proper Cleanup**
   - Try-finally blocks restore original state
   - Prevents test interference

5. **Mock Isolation**
   - Each test has isolated mocks
   - No shared state between tests

6. **Comprehensive Assertions**
   - Verify status codes
   - Check response bodies
   - Validate mock call arguments
   - Confirm side effects

---

## Troubleshooting

### Async Tests Not Running
If async tests are skipped, ensure:
- `pytest-asyncio` is installed
- `asyncio_mode = auto` is in pytest.ini

### Mock Not Working
If mocks aren't applied:
- Check patch path matches import path
- Ensure patch is before import for module-level mocks
- Verify async methods use AsyncMock

### Tests Interfering
If tests affect each other:
- Check that mocks are properly cleaned up
- Use try-finally blocks
- Verify no shared mutable state

---

## Future Test Enhancements

1. **Property-Based Testing**
   - Use `hypothesis` for generated test data
   - Test with random but valid inputs

2. **Contract Testing**
   - Validate API contract with Management API
   - Use tools like Pact

3. **Performance Tests**
   - Add benchmarks for key operations
   - Monitor memory usage

4. **Security Tests**
   - Add tests for injection attacks
   - Validate input sanitization requirements

5. **Integration Tests**
   - Test with real Kafka (testcontainers)
   - Full stack integration tests

---

## Summary

This test suite provides comprehensive coverage of the web-server application with:

- **39 passing tests** covering all major functionality
- **Proper mocking** of external dependencies (Kafka, HTTP client)
- **Edge case coverage** for unusual inputs
- **Error scenario testing** for resilience
- **Integration tests** for end-to-end flows
- **Clear documentation** of expected behaviors

The tests serve as both validation and documentation of the system's behavior.
