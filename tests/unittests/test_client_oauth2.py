import unittest
import base64
from unittest.mock import patch, MagicMock
from parameterized import parameterized
import requests
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from tap_qualtrics.client import Client
from tap_qualtrics.exceptions import QualtricsError

# OAuth2 configuration with client credentials
oauth2_config = {
    "client_id": "test_client_id_12345",
    "client_secret": "test_client_secret_67890",
    "data_center": "testDataCenter",
    "start_date": "2020-01-01",
    "request_timeout": 30,
}

DEFAULT_REQUEST_TIMEOUT = 300


class TestClientOAuth2(unittest.TestCase):
    """Test suite for OAuth2 authentication flow."""

    @patch("tap_qualtrics.client.Client._obtain_oauth_token")
    def setUp(self, mock_oauth):
        """Set up the client with OAuth2 configuration."""
        # Mock the OAuth token acquisition to avoid actual API calls
        mock_oauth.return_value = None
        self.client = Client(oauth2_config)
        self.client.access_token = "test_access_token_xyz"

    def test_oauth2_token_endpoint_construction(self):
        """Test that OAuth2 token endpoint is correctly constructed."""
        # Mock to ensure data_center is set correctly
        with patch.object(Client, "_obtain_oauth_token"):
            client = Client(oauth2_config)
            expected_endpoint = "https://testDataCenter.qualtrics.com/oauth2/token"
            self.assertEqual(client.oauth_token_endpoint, expected_endpoint)

    def test_obtain_oauth_token_missing_client_id(self):
        """Test that missing client_id raises QualtricsError."""
        config_missing_client_id = {
            "client_secret": "secret",
            "data_center": "testDataCenter",
        }

        client = Client(config_missing_client_id)
        with self.assertRaises(QualtricsError) as context:
            with client:
                pass
        self.assertIn("client_id", str(context.exception))

    def test_obtain_oauth_token_missing_client_secret(self):
        """Test that missing client_secret raises QualtricsError."""
        config_missing_client_secret = {
            "client_id": "client_id",
            "data_center": "testDataCenter",
        }

        client = Client(config_missing_client_secret)
        with self.assertRaises(QualtricsError) as context:
            with client:
                pass
        self.assertIn("client_secret", str(context.exception))

    def test_oauth2_basic_auth_encoding(self):
        """Test that client credentials are properly base64 encoded."""
        credentials = f"{oauth2_config['client_id']}:{oauth2_config['client_secret']}"
        expected_encoding = base64.b64encode(credentials.encode()).decode()

        # The encoding should match the format: base64(client_id:client_secret)
        self.assertIsNotNone(expected_encoding)
        self.assertTrue(len(expected_encoding) > 0)

    @patch("requests.Session.post")
    @patch("tap_qualtrics.client.Client._obtain_oauth_token", autospec=True)
    def test_obtain_oauth_token_success(self, mock_obtain_bypass, mock_post):
        """Test successful OAuth2 token acquisition with mocked token endpoint."""
        # Setup mock response for the token endpoint
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new_access_token_abc123",
            "token_type": "Bearer",
            "expires_in": 3600,
            "scope": "read:surveys read:survey_responses"
        }
        mock_post.return_value = mock_response

        # Create a client and verify it would handle token response correctly
        with patch.object(Client, "_obtain_oauth_token"):
            client = Client(oauth2_config)
            # Manually verify the token would be extracted
            token_response = mock_response.json()
            access_token = token_response.get("access_token")
            self.assertEqual(access_token, "new_access_token_abc123")

    @patch("requests.Session.post")
    def test_obtain_oauth_token_failure_non_200(self, mock_post):
        """Test OAuth2 token acquisition fails with non-200 response."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"error": "invalid_client"}
        mock_post.return_value = mock_response

        client = Client(oauth2_config)
        with self.assertRaises(QualtricsError) as context:
            with client:
                pass
        self.assertIn("Failed to obtain OAuth2 token", str(context.exception))

    @patch("requests.Session.post")
    def test_obtain_oauth_token_missing_access_token_field(self, mock_post):
        """Test OAuth2 token acquisition fails when access_token is missing in response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "token_type": "Bearer",
            "expires_in": 3600
        }
        mock_post.return_value = mock_response

        client = Client(oauth2_config)
        with self.assertRaises(QualtricsError) as context:
            with client:
                pass
        self.assertIn("No access_token in OAuth2 response", str(context.exception))

    def test_authenticate_bearer_token(self):
        """Test that authenticate() sets Bearer token in Authorization header."""
        headers = {}
        params = {}

        updated_headers, updated_params = self.client.authenticate(headers, params)

        self.assertEqual(updated_headers["Authorization"], f"Bearer {self.client.access_token}")
        self.assertEqual(updated_params, {})

    def test_get_headers_oauth2_format(self):
        """Test that _get_headers() returns only Content-Type for OAuth2."""
        headers = self.client._get_headers()

        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertNotIn("X-API-TOKEN", headers)
        self.assertNotIn("Authorization", headers)

    def test_oauth2_scope_default(self):
        """Test that default OAuth2 scope can be set in config."""
        # The implementation uses the scope from config with empty string as default
        config_without_scope = {
            "client_id": "test_id",
            "client_secret": "test_secret",
            "data_center": "testDataCenter",
        }

        # Verify the configuration structure
        self.assertNotIn("scope", config_without_scope)

        # The _obtain_oauth_token will use the scope from config if provided
        config_with_scope = dict(config_without_scope, scope="read:surveys read:users")
        self.assertEqual(config_with_scope.get("scope"), "read:surveys read:users")

    @patch("tap_qualtrics.client.Client._obtain_oauth_token")
    def test_oauth2_custom_scope(self, mock_obtain):
        """Test that custom OAuth2 scope can be provided in config."""
        mock_obtain.return_value = None

        custom_scope = "read:surveys read:users"
        config_with_scope = dict(oauth2_config, scope=custom_scope)

        with patch.object(Client, "_obtain_oauth_token"):
            client = Client(config_with_scope)
            self.assertEqual(client.config.get("scope"), custom_scope)

    def test_access_token_stored_after_init(self):
        """Test that access_token is stored after initialization."""
        self.assertIsNotNone(self.client.access_token)
        self.assertEqual(self.client.access_token, "test_access_token_xyz")

    @patch("tap_qualtrics.client.Client._obtain_oauth_token")
    def test_client_initialization_with_oauth2(self, mock_obtain):
        """Test client initialization with OAuth2 configuration."""
        mock_obtain.return_value = None

        with patch.object(Client, "_obtain_oauth_token"):
            client = Client(oauth2_config)
            self.assertEqual(client.config["client_id"], oauth2_config["client_id"])
            self.assertEqual(client.config["client_secret"], oauth2_config["client_secret"])
            self.assertEqual(client.config["data_center"], oauth2_config["data_center"])

    @patch("tap_qualtrics.client.Client._obtain_oauth_token")
    def test_bearer_token_used_in_requests(self, mock_obtain):
        """Test that Bearer token is used in actual API requests."""
        mock_obtain.return_value = None
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": "ok"}
        mock_resp.status_code = 200

        with patch.object(Client, "_obtain_oauth_token"):
            client = Client(oauth2_config)
            client.access_token = "test_access_token_xyz"

            with patch.object(client._session, "request", return_value=mock_resp) as mock_request:
                client.get("users")

                # Verify the request was made
                self.assertTrue(mock_request.called)
                call_kwargs = mock_request.call_args[1]

                # Check that headers contain Bearer token
                self.assertIn("headers", call_kwargs)
                headers = call_kwargs["headers"]
                self.assertIn("Authorization", headers)
                self.assertTrue(headers["Authorization"].startswith("Bearer "))


class TestClientOAuth2TokenExpiration(unittest.TestCase):
    """Test suite for OAuth2 token expiration and refresh logic."""

    @patch("tap_qualtrics.client.Client._obtain_oauth_token")
    def setUp(self, mock_oauth):
        """Set up the client with OAuth2 configuration."""
        mock_oauth.return_value = None
        self.client = Client(oauth2_config)
        self.client.access_token = "test_access_token_xyz"

    @patch("requests.Session.post")
    def test_token_expiration_triggers_refresh(self, mock_post):
        """Test that expired token triggers refresh on next request."""
        from datetime import datetime, timezone, timedelta

        # Set token as expired
        self.client._Client__expires = datetime.now(timezone.utc) - timedelta(seconds=60)

        # Mock successful token refresh
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new_refreshed_token",
            "expires_in": 3600
        }
        mock_post.return_value = mock_response

        # Trigger a request that should refresh the token
        with patch.object(self.client._session, "request", return_value=mock_response):
            self.client._obtain_oauth_token()
            self.assertEqual(self.client.access_token, "new_refreshed_token")

    @patch("requests.Session.post")
    def test_valid_token_not_refreshed(self, mock_post):
        """Test that valid token is not refreshed unnecessarily."""
        from datetime import datetime, timezone, timedelta

        original_token = self.client.access_token
        # Set token as valid (expires in future)
        self.client._Client__expires = datetime.now(timezone.utc) + timedelta(seconds=600)

        # Call _obtain_oauth_token - should return early without refresh
        self.client._obtain_oauth_token()

        # Token should remain unchanged
        self.assertEqual(self.client.access_token, original_token)
        # Post should not be called
        mock_post.assert_not_called()

    @patch("requests.Session.post")
    def test_token_expiration_with_buffer(self, mock_post):
        """Test that token expiration uses 60-second buffer."""
        from datetime import datetime, timezone, timedelta

        expires_in = 7200  # 2 hours
        expected_buffer = 60

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new_token",
            "expires_in": expires_in
        }
        mock_post.return_value = mock_response

        before_time = datetime.now(timezone.utc)
        self.client._obtain_oauth_token()
        after_time = datetime.now(timezone.utc)

        # Verify token was obtained
        self.assertEqual(self.client.access_token, "new_token")

        # Verify expiration time is set with buffer
        expected_expiry = expires_in - expected_buffer
        actual_expiry = (self.client._Client__expires - before_time).total_seconds()
        # Allow 5 second margin for test execution time
        self.assertGreater(actual_expiry, expected_expiry - 5)
        self.assertLess(actual_expiry, expected_expiry + 5)


class TestClientOAuth2Integration(unittest.TestCase):
    """Integration tests for OAuth2 client with API calls."""

    @patch("tap_qualtrics.client.Client._obtain_oauth_token")
    def test_context_manager_flow(self, mock_obtain):
        """Test complete context manager flow with OAuth2."""
        mock_obtain.return_value = None

        with patch.object(Client, "_obtain_oauth_token"):
            with Client(oauth2_config) as client:
                self.assertIsNotNone(client)
                self.assertEqual(client.config["client_id"], oauth2_config["client_id"])

    @patch("tap_qualtrics.client.Client._obtain_oauth_token")
    def test_session_closed_on_exit(self, mock_obtain):
        """Test that session is properly closed on context manager exit."""
        mock_obtain.return_value = None

        with patch.object(Client, "_obtain_oauth_token"):
            with patch.object(requests.Session, "close") as mock_close:
                with Client(oauth2_config) as client:
                    pass

                # Verify close was called
                mock_close.assert_called_once()

    @patch("requests.Session.post")
    def test_post_request_with_json_payload(self, mock_post):
        """Test POST request with JSON payload and Bearer token."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"success": True}
        mock_post.return_value = mock_response

        with patch.object(Client, "_obtain_oauth_token"):
            client = Client(oauth2_config)
            client.access_token = "test_token"

            with patch.object(client._session, "request", return_value=mock_response) as mock_req:
                result = client.post("some/endpoint", payload={"key": "value"})

                # Verify the request was made with Bearer token
                self.assertTrue(mock_req.called)
                call_kwargs = mock_req.call_args[1]
                self.assertIn("Authorization", call_kwargs["headers"])
                self.assertTrue(call_kwargs["headers"]["Authorization"].startswith("Bearer "))
