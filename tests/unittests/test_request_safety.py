"""Regression tests for credential destination safety and pagination progress."""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tap_qualtrics.client import Client
from tap_qualtrics.exceptions import QualtricsError
from tap_qualtrics.streams.abstracts import FullTableStream


CONFIG = {
    "data_center": "sjc1",
    "client_id": "test-client",
    "client_secret": "test-secret",
}
ORIGIN = "https://sjc1.qualtrics.com"


@pytest.mark.parametrize("data_center", [
    None, "", 42, "attacker.example/", "sjc1.qualtrics.com", "https://sjc1",
    "user@attacker.example", "sjc1:443", "sjc1/path", "sjc1?query",
    "sjc1#fragment", "sjc1\\host", "sjc1\n", "-sjc1", "sjc1-", "a" * 64,
])
def test_invalid_data_center_rejected_before_network(data_center):
    with patch("tap_qualtrics.client.requests.Session") as session:
        with pytest.raises(QualtricsError, match="Invalid data_center"):
            Client({**CONFIG, "data_center": data_center})
    session.assert_not_called()


@pytest.mark.parametrize("data_center", ["sjc1", "iad1", "eu-west-1", "testDataCenter", "a"])
def test_valid_data_center_label(data_center):
    client = Client({**CONFIG, "data_center": data_center})
    client._validate_url(client.base_url)
    client._validate_url(client.oauth_token_endpoint)


@pytest.mark.parametrize("url", [
    "https://attacker.example/page",
    "https://sjc1.qualtrics.com.attacker.example/page",
    "https://iad1.qualtrics.com/page",
    "http://sjc1.qualtrics.com/page",
    "https://sjc1.qualtrics.com:444/page",
    "https://sjc1.qualtrics.com:invalid/page",
    "https://user@sjc1.qualtrics.com/page",
    "https://user:password@sjc1.qualtrics.com/page",
    "https://sjc1.qualtrics.com@attacker.example/page",
    "https://sjc1.qualtrics.com\\@attacker.example/page",
    "https://sjc1.qualtrics.com/\npage",
    "https://sjc1.qualtrics.com/page#fragment",
    "https://[invalid/page",
    "//sjc1.qualtrics.com/page", "/API/v3/page", 42,
])
@pytest.mark.parametrize("method", ["get", "post", "get_file"])
def test_untrusted_full_url_rejected_before_authentication(url, method):
    client = Client(CONFIG)
    with patch.object(client, "_obtain_oauth_token") as auth, \
            patch.object(client._session, "request") as request:
        with pytest.raises(QualtricsError, match="Refusing request"):
            getattr(client, method)("surveys", full_url=url)
    auth.assert_not_called()
    request.assert_not_called()


@pytest.mark.parametrize("url", [
    ORIGIN + "/API/v3/surveys?offset=100",
    "https://SJC1.qualtrics.com/API/v3/surveys",
    ORIGIN + ":443/API/v3/surveys",
])
def test_same_origin_pagination_succeeds(url):
    client = Client(CONFIG)
    response = MagicMock(status_code=200)
    response.json.return_value = {"result": {"elements": []}}
    with patch.object(client, "_obtain_oauth_token"), \
            patch.object(client._session, "request", return_value=response) as request:
        assert client.get("surveys", full_url=url) == response.json.return_value
    assert request.call_args.args == ("GET", url)
    assert request.call_args.kwargs["allow_redirects"] is False
    assert "Authorization" in request.call_args.kwargs["headers"]


@pytest.mark.parametrize("flow", ["client_credentials", "authorization_code"])
@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_oauth_redirects_not_followed(flow, status):
    client = Client({**CONFIG, "auth_method": flow, "refresh_token": "test-refresh"})
    response = MagicMock(status_code=status)
    response.headers = {"Location": "https://attacker.example/token"}
    with patch.object(client._session, "post", return_value=response) as post:
        with pytest.raises(QualtricsError, match=f"HTTP {status}"):
            client._obtain_oauth_token()
    post.assert_called_once()
    assert post.call_args.args == (ORIGIN + "/oauth2/token",)
    assert post.call_args.kwargs["allow_redirects"] is False
    response.json.assert_not_called()
    assert client.access_token is None


@pytest.mark.parametrize("flow", ["client_credentials", "authorization_code"])
def test_oauth_destination_checked_at_request_time(flow):
    client = Client({**CONFIG, "auth_method": flow, "refresh_token": "test-refresh"})
    client.oauth_token_endpoint = "https://attacker.example/token"
    with patch.object(client._session, "post") as post:
        with pytest.raises(QualtricsError, match="Refusing request"):
            client._obtain_oauth_token()
    post.assert_not_called()


@pytest.mark.parametrize("method", ["GET", "POST"])
@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_api_redirects_are_errors(method, status):
    client = Client(CONFIG)
    response = MagicMock(status_code=status)
    response.json.return_value = {}
    with patch.object(client, "_obtain_oauth_token"), \
            patch.object(client._session, "request", return_value=response) as request:
        with pytest.raises(QualtricsError):
            client.make_request(method, ORIGIN + "/API/v3/surveys")
    request.assert_called_once()
    assert request.call_args.kwargs["allow_redirects"] is False


@pytest.mark.parametrize("links,expected_requests", [
    ([ORIGIN + "/page2", ORIGIN + "/page2"], 2),
    ([ORIGIN + "/page2", ORIGIN + "/page3", ORIGIN + "/page2"], 3),
    ([{"bad": "link"}], 1),
])
def test_repeated_or_invalid_pagination_fails_visibly(links, expected_requests):
    client = MagicMock(page_size=100)
    client.get.side_effect = [
        {"result": {"elements": [{"id": index}], "nextPage": link}}
        for index, link in enumerate(links)
    ]
    stream = FullTableStream(client=client)
    with pytest.raises(QualtricsError, match="Invalid or repeated pagination URL"):
        list(stream._paginate("surveys"))
    assert client.get.call_count == expected_requests


def test_pagination_rejects_external_link_without_second_request():
    client = Client(CONFIG)
    response = MagicMock(status_code=200)
    response.json.return_value = {
        "result": {"elements": [], "nextPage": "https://attacker.example/page"},
    }
    with patch.object(client, "_obtain_oauth_token") as auth, \
            patch.object(client._session, "request", return_value=response) as request:
        with pytest.raises(QualtricsError, match="Refusing request"):
            list(FullTableStream(client=client)._paginate("surveys"))
    assert auth.call_count == 1
    request.assert_called_once()


@pytest.mark.parametrize("name", [
    "survey_languages", "survey_options", "survey_translations", "survey_versions",
])
def test_new_schemas_have_trailing_newline(name):
    schema = Path(__file__).resolve().parents[2] / "tap_qualtrics" / "schemas" / f"{name}.json"
    assert schema.read_bytes().endswith(b"\n")
