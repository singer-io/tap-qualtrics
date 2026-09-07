import unittest
import requests
from unittest.mock import patch, MagicMock
from parameterized import parameterized
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from tap_qualtrics.client import Client
from tap_qualtrics.exceptions import (
    QualtricsBadRequestError,
    QualtricsConflictError,
    QualtricsForbiddenError,
    QualtricsNotFoundError,
    QualtricsUnauthorizedError,
)


default_config = {
    "clientId": "dummy_client",
    "clientSecret": "dummy_secret",
    "dataCenter": "iad1",
    "start_date": "2020-01-01",
    "request_timeout": 30,
}

DEFAULT_REQUEST_TIMEOUT = 300

class MockResponse:
    """Mocked standard HTTPResponse to test error handling."""

    def __init__(
        self, status_code, resp = "", content=[""], headers=None, raise_error=True, text={}
    ):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"

    def raise_for_status(self):
        """If an error occur, this method returns a HTTPError object.

        Raises:
            requests.HTTPError: Mock http error.

        Returns:
            int: Returns status code if not error occurred.
        """
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("mock sample message")

    def json(self):
        """Returns a JSON object of the result."""
        return self.text

class TestClient(unittest.TestCase):

    def setUp(self):
        """Set up the client with default configuration."""
        self.client = Client(default_config)

    @parameterized.expand([    
        ["empty value", "", DEFAULT_REQUEST_TIMEOUT],
        ["string value", "12", 12.0],
        ["integer value", 10, 10.0],
        ["float value", 20.0, 20.0],
        ["zero value", 0, DEFAULT_REQUEST_TIMEOUT]
    ])
    def test_client_initialization(self, test_name, input_value, expected_value):
        cfg = dict(default_config, request_timeout=input_value)
        client = Client(cfg)
        expected = expected_value if input_value else DEFAULT_REQUEST_TIMEOUT
        assert client.request_timeout == expected

    def test_client_base_url(self):
        client = Client(default_config)
        assert client.base_url == "https://iad1.qualtrics.com/API/v3"

    @patch("tap_qualtrics.client.Client._obtain_oauth_token")
    def test_client_auth_header(self, mock_oauth):
        mock_oauth.return_value = None
        client = Client(default_config)
        headers = client._get_headers()
        assert "Content-Type" in headers
        assert "X-API-TOKEN" not in headers

    @patch("tap_qualtrics.client.Client._obtain_oauth_token")
    def test_client_get_calls_make_request(self, mock_oauth):
        mock_oauth.return_value = None
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": "ok"}
        mock_resp.status_code = 200
        with patch.object(self.client._session, "request", return_value=mock_resp):
            result = self.client.get("users")
        assert result == {"data": "ok"}

    @patch("tap_qualtrics.client.Client._obtain_oauth_token")
    def test_client_post_calls_make_request(self, mock_oauth):
        mock_oauth.return_value = None
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"created": True}
        mock_resp.status_code = 200
        with patch.object(self.client._session, "request", return_value=mock_resp):
            result = self.client.post("users", payload={"key": "value"})
        assert result == {"created": True}

    @parameterized.expand([
        ["400 error", 400, MockResponse(400), QualtricsBadRequestError, "A validation exception has occurred."],
        ["401 error", 401, MockResponse(401), QualtricsUnauthorizedError, "The access token provided is expired, revoked, malformed or invalid for other reasons."],
        ["403 error", 403, MockResponse(403), QualtricsForbiddenError, "You are missing the following required scopes: read"],
        ["404 error", 404, MockResponse(404), QualtricsNotFoundError, "The resource you have specified cannot be found."],
        ["409 error", 409, MockResponse(409), QualtricsConflictError, "The API request cannot be completed because the requested operation would conflict with an existing item."],
    ])
    def test_make_request_http_failure_without_retry(self, test_name, error_code, mock_response, error, error_message):
        with patch.object(self.client._session, "request", return_value=mock_response):
            with self.assertRaises(error) as e:
                self.client._make_request("GET", "https://api.example.com/resource")
        expected_error_message = f"HTTP-error-code: {error_code}, Error: {error_message}"
        self.assertEqual(str(e.exception), expected_error_message)

    @parameterized.expand([
        ["ConnectionResetError", ConnectionResetError],
        ["ConnectionError", ConnectionError],
        ["ChunkedEncodingError", ChunkedEncodingError],
        ["Timeout", Timeout],
    ])
    @patch("time.sleep")
    def test_make_request_other_failure_with_retry(self, test_name, error, mock_sleep):
        with patch.object(self.client._session, "request", side_effect=error) as mock_request:
            with self.assertRaises(error):
                self.client._make_request("GET", "https://api.example.com/resource")
            self.assertGreater(mock_request.call_count, 1)

    @patch("tap_qualtrics.client.Client._obtain_oauth_token")
    @patch("time.sleep")
    def test_poll_export_success(self, mock_sleep, mock_oauth):
        mock_oauth.return_value = None
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"result": {"status": "complete", "fileId": "f1"}}
        with patch.object(self.client._session, "request", return_value=mock_resp):
            result = self.client.poll_export("audit-exports/abc123")
        assert result["result"]["fileId"] == "f1"

    @patch("tap_qualtrics.client.Client._obtain_oauth_token")
    @patch("time.sleep")
    def test_poll_export_timeout(self, mock_sleep, mock_oauth):
        mock_oauth.return_value = None
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"result": {"status": "inProgress"}}
        from tap_qualtrics.exceptions import QualtricsError
        with patch.object(self.client._session, "request", return_value=mock_resp):
            with self.assertRaises(QualtricsError):
                self.client.poll_export("audit-exports/abc123", max_attempts=2, interval=0)
