from dataclasses import dataclass
from typing import Any, Callable, Self

from .json_api_types import APIErrorResponse, Error


@dataclass
class APIException(Exception):
    """
    Base exception class.
    These errors (and their provided details) are reported to the user, so be careful.
    """

    def __init__(self, error: Error) -> None:
        super().__init__(error.detail)
        self.error = error

    @classmethod
    def _create_code(cls) -> str:
        return cls.__name__

    @classmethod
    def _create_title_from_doc(cls) -> str:
        return cls.__doc__ or ""


@dataclass
class APIExceptions(Exception):
    # HTTP status code
    status: int
    exceptions: list[APIException]


class APIInternalError(APIException):
    """API internal error"""

    @classmethod
    def create(cls, detail: str) -> Self:
        return cls(
            Error(
                status="500",
                code=cls._create_code(),
                title=cls._create_title_from_doc(),
                detail=detail,
            )
        )


def get_status_code_and_errors(exception: Exception) -> tuple[int, list[Error]]:
    match exception:
        case APIExceptions():
            exceptions = exception
        case APIException():
            exceptions = APIExceptions(
                status=int(exception.error.status), exceptions=[exception]
            )
        case _:
            exceptions = APIExceptions(
                status=500,
                exceptions=[APIInternalError.create("Internal server error")],
            )
    return exceptions.status, [exc.error for exc in exceptions.exceptions]


def get_exceptions(
    get_exception: Callable[[Error], APIException],
    status_code: int,
    content: dict[str, Any],
) -> APIException | APIExceptions:
    error_response = APIErrorResponse.model_validate(content)
    exceptions = [get_exception(error) for error in error_response.errors]
    if len(exceptions) == 1:
        return exceptions[0]
    else:
        return APIExceptions(status=status_code, exceptions=exceptions)
