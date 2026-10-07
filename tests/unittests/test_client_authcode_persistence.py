import base64
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from tap_qualtrics.client import Client, parse_grant_types
from tap_qualtrics.exceptions import QualtricsError


class TestParseGrantTypes(unittest.TestCase):

    def test_empty_value_returns_empty_set(self):
        self.assertEqual(parse_grant_types(None), set())

    def test_string_value_splits_and_strips(self):
        self.assertEqual(
            parse_grant_types("authorization_code, refresh_token ,"),
            {"authorization_code", "refresh_token"},
        )

    def test_non_string_value_is_stringified(self):
        self.assertEqual(parse_grant_types(123), {"123"})


class TestAuthorizationCodeClientHelpers(unittest.TestCase):

    def _config(self, **overrides):
        config = {
            "client_id": "cid",
            "client_secret": "secret",
            "scope": "manage:all",
            "auth_method": "authorization_code",
            "data_center": "sjc1",
            "redirect_uri": "https://connector.qlik.com/auth/oauth/v3.htm",
            "refresh_token": "old-refresh",
            "access_token": "old-access",
            "start_date": "2026-01-01T00:00:00Z",
        }
        config.update(overrides)
        return config

    def test_fork_copies_config_path_access_token_and_expiry(self):
        client = Client(self._config(), config_path="config.json")
        client._Client__expires = datetime.now(timezone.utc) + timedelta(minutes=5)

        clone = client.fork()

        self.assertEqual(clone.config_path, Path("config.json"))
        self.assertEqual(clone.access_token, "old-access")
        self.assertEqual(clone._Client__expires, client._Client__expires)

    def test_build_basic_auth_headers_encodes_credentials(self):
        client = Client(self._config(auth_method="client_credentials"))

        headers = client._build_basic_auth_headers()

        expected = base64.b64encode(b"cid:secret").decode()
        self.assertEqual(headers["Authorization"], f"Basic {expected}")

    def test_persist_tokens_updates_dict_without_path(self):
        config = self._config()
        client = Client(config)

        client._persist_authorization_code_tokens("new-access", None)

        self.assertEqual(config["access_token"], "new-access")
        self.assertEqual(config["refresh_token"], "old-refresh")

    def test_persist_tokens_updates_config_file(self):
        config = self._config()
        temp_path = Path(tempfile.gettempdir()) / "tap_qualtrics_auth_persist_test.json"
        temp_path.write_text(json.dumps(config), encoding="utf-8")
        client = Client(config, config_path=str(temp_path))

        client._persist_authorization_code_tokens("new-access", "new-refresh")

        saved = json.loads(temp_path.read_text(encoding="utf-8"))
        self.assertEqual(saved["access_token"], "new-access")
        self.assertEqual(saved["refresh_token"], "new-refresh")

    def test_refresh_requires_refresh_token(self):
        client = Client(self._config(refresh_token="", access_token=None))

        with self.assertRaises(QualtricsError) as context:
            client._refresh_authorization_code_token()

        self.assertIn("refresh_token", str(context.exception))

    def test_refresh_raises_on_non_200(self):
        client = Client(self._config(access_token=None))
        client._session = MagicMock()
        response = MagicMock()
        response.status_code = 400
        client._session.post.return_value = response

        with self.assertRaises(QualtricsError) as context:
            client._refresh_authorization_code_token()

        self.assertIn("Failed to refresh OAuth2 token", str(context.exception))

    def test_refresh_raises_when_access_token_missing(self):
        client = Client(self._config(access_token=None))
        client._session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"refresh_token": "new-refresh", "expires_in": 3600}
        client._session.post.return_value = response

        with self.assertRaises(QualtricsError) as context:
            client._refresh_authorization_code_token()

        self.assertIn("No access_token", str(context.exception))

    def test_refresh_persists_new_tokens(self):
        config = self._config(access_token=None)
        client = Client(config)
        client._session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {
            "access_token": "new-access",
            "refresh_token": "new-refresh",
            "expires_in": 3600,
        }
        client._session.post.return_value = response

        client._refresh_authorization_code_token()

        post_data = client._session.post.call_args.kwargs["data"]
        self.assertEqual(
            post_data,
            {
                "grant_type": "refresh_token",
                "client_id": "cid",
                "client_secret": "secret",
                "refresh_token": "old-refresh",
                "redirect_uri": "https://connector.qlik.com/auth/oauth/v3.htm",
            },
        )
        self.assertEqual(config["access_token"], "new-access")
        self.assertEqual(config["refresh_token"], "new-refresh")
        self.assertGreater(client._Client__expires, datetime.now(timezone.utc))

    @patch.object(Client, "_refresh_authorization_code_token")
    def test_obtain_oauth_token_uses_configured_access_token(self, mock_refresh):
        client = Client(self._config())

        client._obtain_oauth_token()

        mock_refresh.assert_not_called()

    @patch.object(Client, "_refresh_authorization_code_token")
    def test_obtain_oauth_token_refreshes_when_access_token_missing(self, mock_refresh):
        client = Client(self._config(access_token=None))

        client._obtain_oauth_token()

        mock_refresh.assert_called_once_with()

    def test_obtain_oauth_token_rejects_missing_credentials(self):
        client = Client(self._config(client_id="", access_token=None))

        with self.assertRaises(QualtricsError) as context:
            client._obtain_oauth_token()

        self.assertIn("client_id", str(context.exception))


if __name__ == "__main__":
    unittest.main()