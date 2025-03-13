from os import environ
from typing import Any, Iterable
from urllib import parse
from fastapi import APIRouter, FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse

from api_utils.exceptions import get_status_code_and_errors
from api_utils.json_api_types import APIErrorResponse, Error


class JSONAPIResponse(JSONResponse):
    media_type = "application/vnd.api+json"


def get_env_var_or_throw(name: str) -> str:
    value = environ.get(name)
    if value is None:
        raise ValueError(f"Environment variable {name} must be set")
    return value


def get_request_url_str(base_url: str, request: Request) -> str:
    return _concat_url_str(base_url, request.url.path, request.url.query)


# Can't accept dict[str, str] for query params because the same key might have multiple values.
def get_url_str(
    base_url: str,
    path: str,
    path_params: dict[str, str] | None = None,
    query_params_list: Iterable[tuple[str, str]] | None = None,
) -> str:
    path_str = path.format_map(
        {
            parse.quote(key, safe=""): parse.quote(value, safe="")
            for key, value in (path_params or {}).items()
        }
    )
    return _concat_url_str(
        base_url, path_str, query=parse.urlencode(list(query_params_list or []))
    )


def _concat_url_str(base_url: str, path: str, query: str) -> str:
    if base_url.endswith("/"):
        base_url = base_url[:-1]
    return base_url + path + ("?" + query if query else "")


def get_api_router_with_defaults() -> APIRouter:
    return APIRouter(
        default_response_class=JSONAPIResponse,
        responses={status.HTTP_422_UNPROCESSABLE_ENTITY: {"model": APIErrorResponse}},
    )


def _exception_handler(request: Request, exc: Exception) -> JSONAPIResponse:
    status_code, errors = get_status_code_and_errors(exc)
    return JSONAPIResponse(
        status_code=status_code,
        content=jsonable_encoder(APIErrorResponse(errors=errors), exclude_unset=True),
    )


# exc should always be RequestValidationError
def _validation_exception_handler(request: Request, exc: Exception) -> JSONAPIResponse:
    # Should never happen, but just in case
    if not isinstance(exc, RequestValidationError):
        return _exception_handler(request, exc)

    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY

    def reshape_error(error: dict[str, Any]) -> Error:
        type: str = error.pop("type")
        msg: str = error.pop("msg")
        loc: tuple = error.pop("loc")
        if loc[0] == "body":
            loc = loc[1:]
        _input = error.pop("input")
        # TODO: turn loc into a valid JSON Pointer
        # The difficulty is that sometimes the last element is int, and when the field is missing it points to the missing field
        # though json pointers must be valid. There might be some other edge cases with which I don't want to deal with for now
        return Error(
            status=str(status_code),
            code=type,
            title=msg,
            meta={**error, "loc": "/" + "/".join([str(name) for name in loc])},
        )

    return JSONAPIResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder(
            APIErrorResponse(errors=[reshape_error(error) for error in exc.errors()]),
            exclude_unset=True,
        ),
    )


def add_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(Exception, _exception_handler)
    app.add_exception_handler(RequestValidationError, _validation_exception_handler)


def set_custom_json_schema(app: FastAPI, title: str, version: str, **args) -> None:
    def custom_openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema
        openapi_schema = get_openapi(
            title=title, version=version, routes=app.routes, **args
        )

        for _, path_schema in openapi_schema["paths"].items():
            for _, method_schema in path_schema.items():
                if "requestBody" in method_schema:
                    content: dict[str, Any] = method_schema["requestBody"]["content"]
                    content["application/vnd.api+json"] = content.pop(
                        "application/json"
                    )
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi  # type: ignore
