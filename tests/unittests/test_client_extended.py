"""Extended tests for tap_qualtrics/client.py covering remaining paths."""
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from tap_qualtrics.client import Client, raise_for_error, wait_if_retry_after
from tap_qualtrics.exceptions import (
    QualtricsError,
    QualtricsRateLimitError,
    QualtricsUnauthorizedError,
)

_OAUTH_CONFIG = {
    "clientId": "cid",
    "clientSecret": "csecret",
    "dataCenter": "iad1",
    "start_date": "2020-01-01",
}


def _client_no_token():
    with patch.object(Client, "_obtain_oauth_token"):
        c = Client(_OAUTH_CONFIG)
    c.access_token = "tok"
    return c


# ---------------------------------------------------------------------------
# raise_for_error
# ---------------------------------------------------------------------------

class TestRaiseForError(unittest.TestCase):

    def test_200_no_raise(self):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {}
        raise_for_error(resp)  # should not raise

    def test_201_no_raise(self):
        resp = MagicMock()
        resp.status_code = 201
        resp.json.return_value = {}
        raise_for_error(resp)

    def test_204_no_raise(self):
        resp = MagicMock()
        resp.status_code = 204
        resp.json.return_value = {}
        raise_for_error(resp)

    def test_error_field_in_json(self):
        resp = MagicMock()
        resp.status_code = 400
        resp.json.return_value = {"error": "custom error message"}
        with self.assertRaises(Exception) as ctx:
            raise_for_error(resp)
        self.assertIn("custom error message", str(ctx.exception))

    def test_json_parse_failure_uses_default_message(self):
        resp = MagicMock()
        resp.status_code = 500
        resp.json.side_effect = ValueError("not json")
        with self.assertRaises(Exception):
            raise_for_error(resp)

    def test_unknown_status_uses_qualtrics_error(self):
        resp = MagicMock()
        resp.status_code = 418  # not in mapping
        resp.json.return_value = {}
        with self.assertRaises(QualtricsError):
            raise_for_error(resp)

    def test_message_field_in_json(self):
        resp = MagicMock()
        resp.status_code = 400
        resp.json.return_value = {"message": "specific message"}
        with self.assertRaises(Exception) as ctx:
            raise_for_error(resp)
        self.assertIn("specific message", str(ctx.exception))


# ---------------------------------------------------------------------------
# Client._obtain_oauth_token
# ---------------------------------------------------------------------------

class TestObtainOAuthToken(unittest.TestCase):

    def test_cached_token_not_refreshed(self):
        c = Client(_OAUTH_CONFIG)
        c.access_token = "existing_token"
        # Set expiry far in the future so it's still valid
        c._Client__expires = datetime.now(timezone.utc) + timedelta(hours=1)
        c._session = MagicMock()
        c._obtain_oauth_token()
        c._session.post.assert_not_called()

    def test_request_exception_raises(self):
        import requests
        c = Client(_OAUTH_CONFIG)
        c.access_token = None
        c._session = MagicMock()
        c._session.post.side_effect = requests.exceptions.RequestException("conn error")
        with self.assertRaises(QualtricsError) as ctx:
            c._obtain_oauth_token()
        self.assertIn("Failed to obtain OAuth2 token", str(ctx.exception))


# ---------------------------------------------------------------------------
# Client.get_file
# ---------------------------------------------------------------------------

class TestGetFile(unittest.TestCase):

    @patch.object(Client, "_obtain_oauth_token")
    def test_get_file_returns_raw_response(self, mock_oauth):
        mock_oauth.return_value = None
        c = Client(_OAUTH_CONFIG)
        c.access_token = "tok"
        raw_resp = MagicMock()
        raw_resp.status_code = 200
        raw_resp.json.return_value = {}
        with patch.object(c._session, "request", return_value=raw_resp):
            result = c.get_file("some/file/path")
        self.assertEqual(result, raw_resp)

    @patch.object(Client, "_obtain_oauth_token")
    def test_get_file_removes_content_type_header(self, mock_oauth):
        mock_oauth.return_value = None
        c = Client(_OAUTH_CONFIG)
        c.access_token = "tok"
        raw_resp = MagicMock()
        raw_resp.status_code = 200
        raw_resp.json.return_value = {}
        with patch.object(c._session, "request", return_value=raw_resp) as mock_req:
            c.get_file("some/file/path")
        _, call_kw = mock_req.call_args
        headers = call_kw.get("headers", {})
        self.assertNotIn("Content-Type", headers)


# ---------------------------------------------------------------------------
# Client.make_request – 401 retry
# ---------------------------------------------------------------------------

class TestMakeRequest401Retry(unittest.TestCase):

    @patch.object(Client, "_obtain_oauth_token")
    def test_401_retries_with_new_token(self, mock_oauth):
        mock_oauth.return_value = None
        c = Client(_OAUTH_CONFIG)
        c.access_token = "old_token"

        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"result": "ok"}

        call_count = [0]

        def side_effect(method, url, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise QualtricsUnauthorizedError("expired token")
            return ok_resp

        with patch.object(c, "_make_request", side_effect=side_effect):
            result = c.make_request("GET", "https://iad1.qualtrics.com/API/v3/users")

        self.assertEqual(call_count[0], 2)


# ---------------------------------------------------------------------------
# Client.poll_export – failed/cancelled status
# ---------------------------------------------------------------------------

class TestPollExportFailedStatus(unittest.TestCase):

    @patch.object(Client, "_obtain_oauth_token")
    def test_failed_status_raises(self, mock_oauth):
        mock_oauth.return_value = None
        c = Client(_OAUTH_CONFIG)
        c.access_token = "tok"
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"result": {"status": "failed"}}
        with patch.object(c._session, "request", return_value=resp):
            with self.assertRaises(QualtricsError):
                c.poll_export("audit-exports/exp1")

    @patch.object(Client, "_obtain_oauth_token")
    def test_cancelled_status_raises(self, mock_oauth):
        mock_oauth.return_value = None
        c = Client(_OAUTH_CONFIG)
        c.access_token = "tok"
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"result": {"status": "cancelled"}}
        with patch.object(c._session, "request", return_value=resp):
            with self.assertRaises(QualtricsError):
                c.poll_export("audit-exports/exp1")

    @patch.object(Client, "_obtain_oauth_token")
    def test_completed_status_ok(self, mock_oauth):
        mock_oauth.return_value = None
        c = Client(_OAUTH_CONFIG)
        c.access_token = "tok"
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"result": {"status": "completed", "fileId": "f1"}}
        with patch.object(c._session, "request", return_value=resp):
            result = c.poll_export("audit-exports/exp1")
        self.assertEqual(result["result"]["fileId"], "f1")


# ---------------------------------------------------------------------------
# Client.make_request – 429 backoff
# ---------------------------------------------------------------------------

class TestMakeRequest429(unittest.TestCase):

    @patch.object(Client, "_obtain_oauth_token")
    @patch("time.sleep")
    def test_429_raises_rate_limit_error(self, mock_sleep, mock_oauth):
        mock_oauth.return_value = None
        c = Client(_OAUTH_CONFIG)
        c.access_token = "tok"
        resp = MagicMock()
        resp.status_code = 429
        resp.headers = {"Retry-After": "0"}
        resp.json.return_value = {}
        with patch.object(c._session, "request", return_value=resp):
            with self.assertRaises(QualtricsRateLimitError):
                c._make_request("GET", "https://iad1.qualtrics.com/API/v3/users")


class TestWaitIfRetryAfter(unittest.TestCase):

    def test_uses_retry_after_header(self):
        response = MagicMock()
        response.headers = {"Retry-After": "12"}
        exc = QualtricsRateLimitError("rate limit", response=response)
        self.assertEqual(wait_if_retry_after({"exception": exc}), 12.0)

    def test_invalid_retry_after_falls_back(self):
        response = MagicMock()
        response.headers = {"Retry-After": "abc"}
        exc = QualtricsRateLimitError("rate limit", response=response)
        self.assertEqual(wait_if_retry_after({"exception": exc}), 5.0)

    def test_missing_response_falls_back(self):
        exc = QualtricsRateLimitError("rate limit")
        self.assertEqual(wait_if_retry_after({"exception": exc}), 5.0)


# ---------------------------------------------------------------------------
# Client context manager
# ---------------------------------------------------------------------------

class TestClientContextManager(unittest.TestCase):

    def test_enter_exit(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "access_token": "tok123",
            "expires_in": 3600,
        }
        with patch("requests.Session.post", return_value=mock_resp):
            with Client(_OAUTH_CONFIG) as c:
                self.assertEqual(c.access_token, "tok123")

    def test_exit_closes_session(self):
        with patch.object(Client, "_obtain_oauth_token"):
            c = Client(_OAUTH_CONFIG)
        c._session = MagicMock()
        c.__exit__(None, None, None)
        c._session.close.assert_called_once()


class TestCheckApiCredentials(unittest.TestCase):

    def test_check_api_credentials_is_noop(self):
        with patch.object(Client, "_obtain_oauth_token"):
            c = Client(_OAUTH_CONFIG)
        # Should not raise; coverage for the `pass` statement
        result = c.check_api_credentials()
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
