import binascii
from typing import Optional, Annotated, List, Tuple, Any
from dataclasses import dataclass
from fastapi import FastAPI, Query, Request, Response, status, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel, Field
import base64
from datetime import datetime
import os

from plugin_utils.runner import call_hooks_until_not_none
from python_opentelemetry_access.telemetry_hooks import (
    Hooks,
    load_hooks,
)
from api_utils.api_utils import (
    JSONAPIResponse,
    add_exception_handlers,
    get_api_router_with_defaults,
    get_request_url_str,
    get_url_str,
    set_custom_json_schema,
)
from api_utils.exceptions import (
    APIException,
    APIUserInputError,
    APIInternalError,
)
from api_utils.json_api_types import (
    APIOKResponseList,
    LinkObject,
    Links,
    Resource as JSONAPIResource,
)
import python_opentelemetry_access.proxy as proxy
import python_opentelemetry_access.otlpjson as otlpjson
import python_opentelemetry_access.util as util


@dataclass
class Settings:
    _proxy: Optional[proxy.Proxy]
    _base_url: Optional[str]

    @property
    def proxy(self) -> proxy.Proxy:
        if self._proxy is None:
            raise APIInternalError(detail="No proxy initialised")
        return self._proxy

    @property
    def base_url(self) -> str:
        if self._base_url is None:
            raise APIInternalError(detail="Base URL must be set")
        return self._base_url


class QueryParams(BaseModel):
    ## Filter parameters
    ## TODO: Expand this with the remaining filter params
    from_time: Optional[datetime] = Field(None)
    to_time: Optional[datetime] = Field(None)

    resource_attributes: list[str] = Field([])
    scope_attributes: list[str] = Field([])
    span_attributes: list[str] = Field([])
    span_name: Optional[str] = Field(None)

    ## TODO: Projection/verbosity parameters??

    ## Pagination parameters
    ## TODO: (Maybe?) expand this with max_results hint

    page_size: Optional[int] = Field(None, gt=0)
    ## NOTE: Should be Optional[Base64Bytes], but this makes /docs
    ##       add a file upload field...
    page_token: Optional[str] = Field(None)


class ResponseNextPageToken(BaseModel):
    next_page_token: str | None


class ResponseMeta(BaseModel):
    page: ResponseNextPageToken


type APIOKResponse = APIOKResponseList[
    otlpjson.OTLPJsonSpanCollection.Representation, ResponseMeta
]


def convert_value(value: str, exception: APIException) -> str | int | float | bool:
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    raise exception


def list_to_dict(values: list[str]) -> util.AttributesFilter:
    result: util.AttributesFilter = {}
    for value in values:
        exception = APIUserInputError(
            title="Malformed Attribute Filter Parameter",
            detail=f"""Attribute filter parameter must be of the shape 'my key="my string value"' or 'my key=value' where value is an int, or float, or boolean. '{value}' is of incorrect shape.""",
        )
        match value.split("="):
            case [key]:
                if key not in result:
                    result[key] = None
            case [key, value]:
                # Not the most natural way to express this, but mypy doesn't agree that the commented out code type checks
                # if (key not in result) or (result[key] is None):
                #     result[key] = []
                # result[key].append(value)
                if key not in result:
                    result[key] = []
                cur_values = result[key]
                if cur_values is None:
                    cur_values = result[key] = []
                cur_values.append(convert_value(value, exception))
            case _:
                raise exception
    return result


settings = Settings(_proxy=None, _base_url=None)

GET_FASTAPI_SECURITY_HOOK_NAME = (
    os.environ.get("RH_TELEMETRY_GET_FASTAPI_SECURITY_HOOK_NAME")
    or "get_fastapi_security"
)
ON_AUTH_HOOK_NAME = os.environ.get("RH_TELEMETRY_ON_AUTH_HOOK_NAME") or "on_auth"

loaded_hooks: dict[str, Hooks] = load_hooks()


async def security_scheme(request: Request) -> Any | None:
    if GET_FASTAPI_SECURITY_HOOK_NAME in loaded_hooks:
        return await call_hooks_until_not_none(
            loaded_hooks[GET_FASTAPI_SECURITY_HOOK_NAME], request
        )
    else:
        return None


async def run_query(
    auth_info: Any,
    request: Request,
    path: str,
    span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]],
    query_params: QueryParams,
) -> APIOKResponse:
    new_page_tokens = []
    span_sets = []

    if query_params.page_token is not None:
        try:
            page_tokens: List[Optional[proxy.PageToken]] = [
                proxy.PageToken(base64.b64decode(token, validate=True))
                for token in query_params.page_token.split(".")
            ]
        except binascii.Error:
            raise util.InvalidPageTokenException()

    else:
        page_tokens = [None]

    resource_attributes = list_to_dict(query_params.resource_attributes)
    scope_attributes = list_to_dict(query_params.scope_attributes)
    span_attributes = list_to_dict(query_params.span_attributes)

    for page_token in page_tokens:
        async for res in settings.proxy.query_spans_page(
            auth_info=auth_info,
            span_ids=span_ids,
            from_time=query_params.from_time,
            to_time=query_params.to_time,
            resource_attributes=resource_attributes,
            scope_attributes=scope_attributes,
            span_attributes=span_attributes,
            span_name=query_params.span_name,
            page_size=query_params.page_size,
            page_token=page_token,
        ):
            if isinstance(res, proxy.PageToken):
                new_page_tokens.append(res)
            else:
                span_sets.append(
                    otlpjson.OTLPJsonSpanCollection.Representation(
                        util.force_jsonlike_dict_iter(res.to_otlp_json_iter())
                    )  # type: ignore
                )

    next_page_token: Optional[str] = (
        None
        if not new_page_tokens
        else ".".join(
            base64.b64encode(token.token).decode("ascii") for token in new_page_tokens
        )
    )

    # # Disable validation for now
    # return APIResponse.model_construct(
    #     None, results=span_sets, next_page_token=next_page_token
    # )

    # query_params = request.query_params
    # query_params

    query_params_list = [
        (key, value)
        for key, value in request.query_params.multi_items()
        if key != "page_token"
    ]
    link_first = get_url_str(
        settings.base_url, path, request.path_params, query_params_list
    )
    link_next = (
        get_url_str(
            settings.base_url,
            path,
            request.path_params,
            query_params_list=query_params_list + [("page_token", next_page_token)],
        )
        if next_page_token is not None
        else None
    )

    return APIOKResponseList[
        otlpjson.OTLPJsonSpanCollection.Representation, ResponseMeta
    ](
        data=[
            JSONAPIResource[
                otlpjson.OTLPJsonSpanCollection.Representation
            ].model_construct(id=None, type="resourceSpans", attributes=span_set)
            for span_set in span_sets
        ],
        links=Links(
            self=get_request_url_str(settings.base_url, request),
            first=link_first,
            next=link_next,
            root=settings.base_url,
        ),
        meta=ResponseMeta(page=ResponseNextPageToken(next_page_token=next_page_token)),
    )


app = FastAPI()
# A solution to make CORS headers appear in error responses too, based on
# https://github.com/fastapi/fastapi/discussions/8027#discussioncomment-5146484
wrapped_app = CORSMiddleware(
    app=app,
    allow_origin_regex=".*",
    # Even though the below allows all things too, it disables returns Access-Control-Allow-Origin=* in the header
    # and borwsers don't allow to use that with withCredentials=True
    # allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=9)
add_exception_handlers(app)

router = get_api_router_with_defaults()


# app. instead of router. because don't want to indicate that could return HTTP error 422
@app.get(
    "/",
    status_code=status.HTTP_200_OK,
    response_model_exclude_unset=True,
    response_class=JSONAPIResponse,
)
async def root(
    auth_info: Annotated[Any, Depends(security_scheme)], request: Request
) -> APIOKResponseList[None, None]:
    if ON_AUTH_HOOK_NAME in loaded_hooks:
        auth_info = await call_hooks_until_not_none(
            loaded_hooks[ON_AUTH_HOOK_NAME], auth_info
        )

    return APIOKResponseList[None, None](
        data=[
            JSONAPIResource[None](
                id="documentation_website",
                type="api_path",
                attributes=None,
                links={"self": get_url_str(settings.base_url, "/docs")},
            ),
            JSONAPIResource[None](
                id="get_spans",
                type="api_path",
                attributes=None,
                links={"self": get_url_str(settings.base_url, "/v1/spans")},
            ),
        ],
        links=Links(
            self=settings.base_url,
            describedby=LinkObject(
                title="OpenAPI schema",
                href=get_url_str(settings.base_url, "/openapi.json"),
            ),
        ),
        meta=None,
    )


@router.get(
    "/v1/spans",
    status_code=status.HTTP_200_OK,
    response_model_exclude_unset=True,
)
async def get_spans(
    auth_info: Annotated[Any, Depends(security_scheme)],
    request: Request,
    response: Response,
    query_params: Annotated[QueryParams, Query()],
) -> APIOKResponse:
    if ON_AUTH_HOOK_NAME in loaded_hooks:
        auth_info = await call_hooks_until_not_none(
            loaded_hooks[ON_AUTH_HOOK_NAME], auth_info
        )

    response.headers["Allow"] = "GET"
    return await run_query(
        auth_info, request, path="/v1/spans", span_ids=None, query_params=query_params
    )


@router.get(
    "/v1/spans/{trace_id}",
    status_code=status.HTTP_200_OK,
    response_model_exclude_unset=True,
)
async def get_trace(
    auth_info: Annotated[Any, Depends(security_scheme)],
    request: Request,
    response: Response,
    trace_id: str,
    query_params: Annotated[QueryParams, Query()],
) -> APIOKResponse:
    if ON_AUTH_HOOK_NAME in loaded_hooks:
        auth_info = await call_hooks_until_not_none(
            loaded_hooks[ON_AUTH_HOOK_NAME], auth_info
        )

    response.headers["Allow"] = "GET"
    return await run_query(
        auth_info,
        request,
        path="/v1/spans/{trace_id}",
        span_ids=[(trace_id, None)],
        query_params=query_params,
    )


@router.get(
    "/v1/spans/{trace_id}/{span_id}",
    status_code=status.HTTP_200_OK,
    response_model_exclude_unset=True,
)
async def get_span(
    auth_info: Annotated[Any, Depends(security_scheme)],
    request: Request,
    response: Response,
    trace_id: str,
    span_id: str,
    query_params: Annotated[QueryParams, Query()],
) -> APIOKResponse:
    if ON_AUTH_HOOK_NAME in loaded_hooks:
        auth_info = await call_hooks_until_not_none(
            loaded_hooks[ON_AUTH_HOOK_NAME], auth_info
        )

    response.headers["Allow"] = "GET"
    return await run_query(
        auth_info,
        request,
        path="/v1/spans/{trace_id}/{span_id}",
        span_ids=[(trace_id, span_id)],
        query_params=query_params,
    )


@router.get(
    "/livez",
    status_code=status.HTTP_200_OK,
)
async def livez() -> str:
    return "OK"


@router.get(
    "/readyz",
    status_code=status.HTTP_200_OK,
)
async def readyz() -> str:
    return "OK"


app.include_router(router)

set_custom_json_schema(app, "Check Telemetry API", "v1")
