import binascii
from collections import defaultdict
from typing import Optional, Annotated, List, Tuple
from dataclasses import dataclass
from fastapi import FastAPI, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import base64
from datetime import datetime

from api_utils.api_utils import (
    JSONAPIResponse,
    add_exception_handlers,
    get_api_router_with_defaults,
    get_env_var_or_throw,
    get_request_url_str,
    get_url_str,
    set_custom_json_schema,
)
from api_utils.exceptions import APIException, APIInternalError
from api_utils.json_api_types import (
    APIOKResponseList,
    Error,
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
    base_url: str

    @property
    def proxy(self) -> proxy.Proxy:
        if self._proxy is None:
            raise APIInternalError.create("No proxy initialised")
        return self._proxy


class QueryParams(BaseModel):
    ## Filter parameters
    ## TODO: Expand this with the remaining filter params
    from_time: Optional[datetime] = Field(None)
    to_time: Optional[datetime] = Field(None)

    resource_attributes: list[str] = []
    scope_attributes: list[str] = []
    span_attributes: list[str] = []

    ## TODO: Projection/verbosity parameters??

    ## Pagination parameters
    ## TODO: (Maybe?) expand this with max_results hint

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


def list_to_dict(values: list[str]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = defaultdict(list)
    for value in values:
        match value.split("="):
            case [key, value]:
                result[key].append(value)
            case _:
                raise APIException(
                    Error(
                        status="400",
                        code="AttributeFilterParameterMalformed",
                        title="Malformed Attribute Filter Parameter",
                        detail=f"Attribute filter parameter must be of the shape 'my awesome key=my awesome value'. '{value}' is of incorrect shape.",
                    )
                )

    return result


settings = Settings(
    _proxy=None, base_url=get_env_var_or_throw("RH_TELEMETRY_API_BASE_URL")
)


async def run_query(
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
            raise util.InvalidPageTokenException.create()
            # raise APIException(
            #     Error(
            #         status="400",
            #         code="PageTokenMalformed",
            #         title="Page Token is in Incorrect Format",
            #         detail=f"Expected page token to be dot (.) separated base 64 encoded strings. '{query_params.page_token}' doesn't comply to that.",
            #     )
            # )

    else:
        page_tokens = [None]

    resource_attributes = list_to_dict(query_params.resource_attributes)
    scope_attributes = list_to_dict(query_params.scope_attributes)
    span_attributes = list_to_dict(query_params.span_attributes)

    for page_token in page_tokens:
        async for res in settings.proxy.query_spans_page(
            span_ids=span_ids,
            from_time=query_params.from_time,
            to_time=query_params.to_time,
            resource_attributes=resource_attributes,
            scope_attributes=scope_attributes,
            span_attributes=span_attributes,
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
app.add_middleware(
    CORSMiddleware,
    # Might need to replace allow_origins=["*"] with the below line, as was done in CheckManager
    # allow_origin_regex=".*",
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
add_exception_handlers(app)

router = get_api_router_with_defaults()


# app. instead of router. because don't want to indicate that could return HTTP error 422
@app.get(
    "/",
    status_code=status.HTTP_200_OK,
    response_model_exclude_unset=True,
    response_class=JSONAPIResponse,
)
async def root(request: Request) -> APIOKResponseList[None, None]:
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
    request: Request, response: Response, query_params: Annotated[QueryParams, Query()]
) -> APIOKResponse:
    response.headers["Allow"] = "GET"
    return await run_query(
        request, path="/v1/spans", span_ids=None, query_params=query_params
    )


@router.get(
    "/v1/spans/{trace_id}",
    status_code=status.HTTP_200_OK,
    response_model_exclude_unset=True,
)
async def get_trace(
    request: Request,
    response: Response,
    trace_id: str,
    query_params: Annotated[QueryParams, Query()],
) -> APIOKResponse:
    response.headers["Allow"] = "GET"
    return await run_query(
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
    request: Request,
    response: Response,
    trace_id: str,
    span_id: str,
    query_params: Annotated[QueryParams, Query()],
) -> APIOKResponse:
    response.headers["Allow"] = "GET"
    return await run_query(
        request,
        path="/v1/spans/{trace_id}/{span_id}",
        span_ids=[(trace_id, span_id)],
        query_params=query_params,
    )


app.include_router(router)

set_custom_json_schema(app, "Check Telemetry API", "v1")
