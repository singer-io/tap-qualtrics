import base64
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional, Tuple

import backoff
import requests
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError as RequestsConnectionError,
    Timeout,
)
from singer import get_logger, metrics

from tap_qualtrics.exceptions import (ERROR_CODE_EXCEPTION_MAPPING,
                                      QualtricsBadGatewayError, QualtricsError,
                                      QualtricsInternalServerError,
                                      QualtricsRateLimitError,
                                      QualtricsServiceUnavailableError,
                                      QualtricsUnauthorizedError)

LOGGER = get_logger()
REQUEST_TIMEOUT = 300
MAX_POLL_ATTEMPTS = 60
POLL_INTERVAL = 5  # seconds between status checks


def wait_if_retry_after(details_or_exception) -> float:
    """Return retry wait seconds from Retry-After header when available."""
    if isinstance(details_or_exception, dict):
        exception = details_or_exception.get("exception")
    else:
        exception = details_or_exception
    response = getattr(exception, "response", None)

    if response is not None and getattr(response, "headers", None):
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return float(retry_after)
            except (TypeError, ValueError):
                pass

    # Fallback when header is absent or invalid.
    return 5.0


def raise_for_error(response: requests.Response) -> None:
    """Raise the appropriate exception for non-2xx responses."""
    try:
        response_json = response.json()
    except (ValueError, AttributeError):
        response_json = {}
    if response.status_code not in [200, 201, 204]:
        if response_json.get("error"):
            message = (
                f"HTTP-error-code: {response.status_code}, "
                f"Error: {response_json.get('error')}"
            )
        else:
            error_message = ERROR_CODE_EXCEPTION_MAPPING.get(
                response.status_code, {}
            ).get("message", "Unknown Error")
            message = (
                f"HTTP-error-code: {response.status_code}, "
                f"Error: {response_json.get('message', error_message)}"
            )
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get(
            "raise_exception", QualtricsError
        )
        raise exc(message, response) from None


class Client:  # pylint: disable=too-many-instance-attributes
    """HTTP client for the Qualtrics API (OAuth2 client credentials auth)."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self.data_center = config.get("data_center")
        self._session = requests.Session()
        self.base_url = f"https://{self.data_center}.qualtrics.com/API/v3"
        config_request_timeout = config.get("request_timeout")
        self.request_timeout = (
            float(config_request_timeout)
            if config_request_timeout
            else REQUEST_TIMEOUT
        )
        self.start_date = config.get("start_date")
        self.page_size = int(config.get("page_size", 100))
        self.access_token = None
        self.__expires = datetime.now(timezone.utc) - timedelta(seconds=10)
        self.oauth_token_endpoint = f"https://{self.data_center}.qualtrics.com/oauth2/token"

    def __enter__(self):
        self._obtain_oauth_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def fork(self) -> "Client":
        """Create a client copy with an independent requests session."""
        clone = Client(self.config)
        clone.access_token = self.access_token
        clone._Client__expires = self.__expires  # pylint: disable=protected-access
        return clone

    def _get_headers(self) -> Dict[str, str]:
        """Get default headers for API requests."""
        return {
            "Content-Type": "application/json"
        }

    def check_api_credentials(self) -> None:
        pass

    def _obtain_oauth_token(self) -> None:
        """Obtain OAuth2 access token using client credentials flow."""
        client_id = self.config.get("client_id")
        client_secret = self.config.get("client_secret")

        if not client_id or not client_secret:
            raise QualtricsError(
                "Missing required OAuth2 credentials: client_id and client_secret"
            )

        # Check if the token is still valid
        if self.access_token and datetime.now(timezone.utc) < self.__expires:
            LOGGER.info("Using cached OAuth2 access token")
            return

        # Encode credentials in Base64 for Basic auth
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Authorization": f"Basic {encoded_credentials}"
        }

        data = {
            "grant_type": "client_credentials",
            "scope": self.config.get("scope", "")
        }

        try:
            response = self._session.post(
                self.oauth_token_endpoint,
                headers=headers,
                data=data,
                timeout=self.request_timeout,
            )
            if response.status_code != 200:
                raise QualtricsError(
                    f"Failed to obtain OAuth2 token: HTTP {response.status_code}"
                )
            token_response = response.json()
            self.access_token = token_response.get("access_token")
            if not self.access_token:
                raise QualtricsError("No access_token in OAuth2 response")

            LOGGER.info("Successfully obtained OAuth2 access token")

            # Set token expiration time and pad a 60 seconds buffer to avoid using an expired token
            expires_in_seconds = token_response.get("expires_in") - 60
            self.__expires = datetime.now(timezone.utc) + timedelta(seconds=expires_in_seconds)

        except requests.exceptions.RequestException as e:
            raise QualtricsError(f"Failed to obtain OAuth2 token: {str(e)}") from e

    def authenticate(self, headers: Dict, params: Dict) -> Tuple[Dict, Dict]:
        """Authenticates the request with OAuth2 Bearer token"""
        headers["Authorization"] = f"Bearer {self.access_token}"
        return headers, params

    def make_request(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        path: Optional[str] = None,
    ) -> Any:
        """
        Sends an HTTP request to the specified API endpoint.
        """
        # Get the access token if it's not already set or expired
        self._obtain_oauth_token()

        params = params or {}
        headers = headers or {}
        body = body or {}
        endpoint = endpoint or f"{self.base_url}/{path}"
        headers, params = self.authenticate(headers, params)
        try:
            return self._make_request(
                method,
                endpoint,
                headers=headers,
                params=params,
                data=body,
                json=json,
                timeout=self.request_timeout
            )
        except QualtricsUnauthorizedError:
            # Token expired mid-sync; force refresh and retry once
            self.access_token = None
            self._obtain_oauth_token()
            headers["Authorization"] = f"Bearer {self.access_token}"
            return self._make_request(
                method,
                endpoint,
                headers=headers,
                params=params,
                data=body,
                json=json,
                timeout=self.request_timeout
            )

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            ConnectionResetError,
            RequestsConnectionError,
            ChunkedEncodingError,
            Timeout,
            QualtricsServiceUnavailableError,
            QualtricsBadGatewayError,
        ),
        max_tries=5,
        factor=2,
    )
    @backoff.on_exception(
        backoff.runtime,
        exception=(QualtricsRateLimitError,),
        max_tries=5,
        value=wait_if_retry_after,
        jitter=None,
    )
    def _make_request(
        self, method: str, url: str, **kwargs
    ) -> requests.Response:
        """Low-level request with retry/backoff."""
        kwargs.setdefault("headers", self._get_headers())
        kwargs.setdefault("timeout", self.request_timeout)
        with metrics.http_request_timer(url):
            response = self._session.request(method.upper(), url, **kwargs)
        raise_for_error(response)
        return response

    def get(
        self,
        path: str,
        params: Optional[Dict] = None,
        full_url: Optional[str] = None,
    ) -> Any:
        """GET request. Uses full_url when provided (for next-page URLs)."""
        url = full_url or f"{self.base_url}/{path}"
        return self.make_request("GET", url, params=params).json()

    def post(
        self,
        path: str,
        payload: Optional[Dict] = None,
        full_url: Optional[str] = None,
    ) -> Any:
        """POST request with JSON body."""
        url = full_url or f"{self.base_url}/{path}"
        return self.make_request("POST", url, json=payload).json()

    def get_file(
        self,
        path: str,
        full_url: Optional[str] = None,
    ) -> requests.Response:
        """GET request returning the raw response (for file downloads)."""
        url = full_url or f"{self.base_url}/{path}"
        headers = dict(self._get_headers())
        # remove Content-Type for binary downloads
        headers.pop("Content-Type", None)
        return self.make_request("GET", url, headers=headers)

    def poll_export(
        self,
        status_path: str,
        max_attempts: int = MAX_POLL_ATTEMPTS,
        interval: int = POLL_INTERVAL,
    ) -> Any:
        """Poll an async export status endpoint until status == 'complete'."""
        for _ in range(max_attempts):
            response = self.get(status_path)
            status = (response.get("result") or {}).get("status", "")
            if status.lower() in ("complete", "completed"):
                return response
            if status.lower() in ("failed", "cancelled", "error"):
                raise QualtricsError(f"Export failed with status: {status}")
            time.sleep(interval)
        raise QualtricsError(f"Export did not complete after {max_attempts} attempts")
