import unittest
from unittest.mock import MagicMock

import requests

from tap_qualtrics.client import QUALTRICS_SCOPE, Client
from tap_qualtrics.exceptions import QualtricsError, QualtricsForbiddenError

BASE_CONFIG = {
    "client_id": "cid",
    "client_secret": "secret",
    "data_center": "iad1",
    "start_date": "2026-01-01T00:00:00Z",
}


def _token_response(access_token="token", expires_in=3600):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "access_token": access_token,
        "expires_in": expires_in,
    }
    return response


def _http_error_response(status_code, payload):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = payload
    response.headers = {}
    return response


class TestClientNegativeAuthAndConfig(unittest.TestCase):

    def test_client_credentials_bad_credentials_raise(self):
        config = dict(BASE_CONFIG)
        response = MagicMock()
        response.status_code = 401
        response.json.return_value = {"error": "invalid_client"}

        client = Client(config)
        client._session.post = MagicMock(return_value=response)

        with self.assertRaises(QualtricsError) as context, client:
            pass

        self.assertIn("Failed to obtain OAuth2 token", str(context.exception))

    def test_wrong_data_center_connection_failure_raises(self):
        config = dict(BASE_CONFIG, data_center="wrongdc")

        client = Client(config)
        client._session.post = MagicMock(
            side_effect=requests.exceptions.ConnectionError("host not found")
        )

        with self.assertRaises(QualtricsError) as context, client:
            pass

        self.assertIn("Failed to obtain OAuth2 token", str(context.exception))

    def test_missing_scope_forbidden_error_client_credentials(self):
        config = dict(BASE_CONFIG)
        client = Client(config)
        client._session.post = MagicMock(return_value=_token_response())
        client._session.request = MagicMock(
            return_value=_http_error_response(
                403,
                {"message": "You are missing the following required scopes: read:survey_responses"},
            )
        )

        with client, self.assertRaises(QualtricsForbiddenError) as context:
            client.get("surveys")

        self.assertIn("required scopes", str(context.exception))

    def test_missing_scope_forbidden_error_authorization_code(self):
        config = dict(
            BASE_CONFIG,
            auth_method="authorization_code",
            access_token="existing-token",
            refresh_token="refresh-token",
        )
        client = Client(config)
        client._session.request = MagicMock(
            return_value=_http_error_response(
                403,
                {"message": "You are missing the following required scopes: read:survey_responses"},
            )
        )

        with client, self.assertRaises(QualtricsForbiddenError) as context:
            client.get("surveys")

        self.assertIn("required scopes", str(context.exception))

    def test_authorization_code_refresh_bad_credentials_raise(self):
        config = dict(
            BASE_CONFIG,
            auth_method="authorization_code",
            access_token=None,
            refresh_token="refresh-token",
            redirect_uri="https://connector.qlik.com/auth/oauth/v3.htm",
        )
        response = MagicMock()
        response.status_code = 401

        client = Client(config)
        client._session.post = MagicMock(return_value=response)

        with self.assertRaises(QualtricsError) as context, client:
            pass

        self.assertIn("Failed to refresh OAuth2 token", str(context.exception))

    def test_client_credentials_branch_posts_expected_payload(self):
        config = dict(BASE_CONFIG, auth_method="client_credentials")
        client = Client(config)
        client._session.post = MagicMock(return_value=_token_response())

        with client:
            pass

        _, kwargs = client._session.post.call_args
        self.assertEqual(kwargs["data"]["grant_type"], "client_credentials")
        self.assertEqual(kwargs["data"]["scope"], QUALTRICS_SCOPE)

    def test_authorization_code_branch_uses_refresh_token_when_access_missing(self):
        config = dict(
            BASE_CONFIG,
            auth_method="authorization_code",
            access_token=None,
            refresh_token="refresh-token",
            redirect_uri="https://connector.qlik.com/auth/oauth/v3.htm",
        )
        client = Client(config)
        client._session.post = MagicMock(return_value=_token_response("new-token"))

        with client:
            pass

        _, kwargs = client._session.post.call_args
        self.assertEqual(kwargs["data"]["grant_type"], "refresh_token")
        self.assertEqual(kwargs["data"]["refresh_token"], "refresh-token")
        self.assertEqual(client.access_token, "new-token")

    def test_authorization_code_branch_skips_refresh_when_access_provided(self):
        config = dict(
            BASE_CONFIG,
            auth_method="authorization_code",
            access_token="existing-token",
            refresh_token="refresh-token",
        )
        client = Client(config)
        client._session.post = MagicMock()

        with client:
            pass

        client._session.post.assert_not_called()

    def test_missing_data_center_is_rejected(self):
        with self.assertRaises(QualtricsError):
            Client({"client_id": "cid", "client_secret": "secret", "start_date": "2026-01-01T00:00:00Z"})

    def test_extra_config_keys_are_accepted(self):
        config = dict(BASE_CONFIG, extra_config_key="extra-value")
        client = Client(config)

        self.assertEqual(client.config["extra_config_key"], "extra-value")


if __name__ == "__main__":
    unittest.main()
