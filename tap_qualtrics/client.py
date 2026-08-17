import time
from typing import Any, Dict, Mapping, Optional, Tuple

import backoff
import requests
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from singer import get_logger, metrics

from tap_qualtrics.exceptions import ERROR_CODE_EXCEPTION_MAPPING, QualtricsError, QualtricsBackoffError

LOGGER = get_logger()
REQUEST_TIMEOUT = 300
MAX_POLL_ATTEMPTS = 60
POLL_INTERVAL = 5  # seconds between status checks


def raise_for_error(response: requests.Response) -> None:
    """Raise the appropriate exception for non-2xx responses."""
    try:
        response_json = response.json()
    except Exception:
        response_json = {}
    if response.status_code not in [200, 201, 204]:
        if response_json.get("error"):
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('error')}"
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


class Client:
    """HTTP client for the Qualtrics API (X-API-TOKEN auth)."""

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._session = session()
        self.base_url = "https://{dataCenter}.qualtrics.com/API/v3/"
        config_request_timeout = config.get("request_timeout")
        self.request_timeout = float(config_request_timeout) if config_request_timeout else REQUEST_TIMEOUT

    def __enter__(self):
        self.check_api_credentials()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def check_api_credentials(self) -> None:
        pass

    def authenticate(self, headers: Dict, params: Dict) -> Tuple[Dict, Dict]:
        """Authenticates the request with the token"""
        headers["Authorization"] = self.config["access_token"]
        return headers, params

    def make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        path: Optional[str] = None
    ) -> Any:
        """
        Sends an HTTP request to the specified API endpoint.
        """
        params = params or {}
        headers = headers or {}
        body = body or {}
        endpoint = endpoint or f"{self.base_url}/{path}"
        headers, params = self.authenticate(headers, params)
        return self.__make_request(
            method, endpoint,
            headers=headers,
            params=params,
            data=body,
            timeout=self.request_timeout
        )

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            ConnectionResetError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            QualtricsBackoffError,
        ),
        max_tries=8,
        factor=3,
    )
    def _make_request(
        self, method: str, url: str, **kwargs
    ) -> requests.Response:
        """Low-level request with retry/backoff."""
        kwargs.setdefault("headers", self._get_headers())
        kwargs.setdefault("timeout", self.request_timeout)
        with metrics.http_request_timer(url):
            response = self._session.request(method.upper(), url, **kwargs)
        if response.status_code == 429:
            raise QualtricsBackoffError("Rate limited (429)")
        raise_for_error(response)
        return response

    def get(self, path: str, params: Optional[Dict] = None, full_url: Optional[str] = None) -> Any:
        """GET request. Uses full_url when provided (for next-page URLs)."""
        url = full_url or f"{self.base_url}/{path}"
        return self._make_request("GET", url, params=params).json()

    def post(self, path: str, payload: Optional[Dict] = None, full_url: Optional[str] = None) -> Any:
        """POST request with JSON body."""
        url = full_url or f"{self.base_url}/{path}"
        return self._make_request("POST", url, json=payload).json()

    def get_file(self, path: str, full_url: Optional[str] = None) -> requests.Response:
        """GET request returning the raw response (for file downloads)."""
        url = full_url or f"{self.base_url}/{path}"
        headers = dict(self._get_headers())
        # remove Content-Type for binary downloads
        headers.pop("Content-Type", None)
        return self._make_request("GET", url, headers=headers)

    def poll_export(
        self,
        status_path: str,
        max_attempts: int = MAX_POLL_ATTEMPTS,
        interval: int = POLL_INTERVAL,
    ) -> Any:
        """Poll an async export status endpoint until status == 'complete'."""
        for attempt in range(max_attempts):
            response = self.get(status_path)
            status = (response.get("result") or {}).get("status", "")
            LOGGER.info("Export %s attempt %d/%d: %s", status_path, attempt + 1, max_attempts, status)
            if status == "complete":
                return response
            if status in ("failed", "cancelled"):
                raise QualtricsError(f"Export failed with status: {status}")
            time.sleep(interval)
        raise QualtricsError(f"Export did not complete after {max_attempts} attempts")

