class QualtricsError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class QualtricsBackoffError(QualtricsError):
    """class representing backoff error handling."""
    pass

class QualtricsBadRequestError(QualtricsError):
    """class representing 400 status code."""
    pass

class QualtricsUnauthorizedError(QualtricsError):
    """class representing 401 status code."""
    pass


class QualtricsForbiddenError(QualtricsError):
    """class representing 403 status code."""
    pass

class QualtricsNotFoundError(QualtricsError):
    """class representing 404 status code."""
    pass

class QualtricsConflictError(QualtricsError):
    """class representing 409 status code."""
    pass

class QualtricsUnprocessableEntityError(QualtricsBackoffError):
    """class representing 422 status code."""
    pass

class QualtricsRateLimitError(QualtricsBackoffError):
    """class representing 429 status code."""
    pass

class QualtricsInternalServerError(QualtricsBackoffError):
    """class representing 500 status code."""
    pass

class QualtricsNotImplementedError(QualtricsBackoffError):
    """class representing 501 status code."""
    pass

class QualtricsBadGatewayError(QualtricsBackoffError):
    """class representing 502 status code."""
    pass

class QualtricsServiceUnavailableError(QualtricsBackoffError):
    """class representing 503 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": QualtricsBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": QualtricsUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": QualtricsForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": QualtricsNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": QualtricsConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": QualtricsUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": QualtricsRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": QualtricsInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": QualtricsNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": QualtricsBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": QualtricsServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}

