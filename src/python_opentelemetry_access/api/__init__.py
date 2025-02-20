from collections import defaultdict

from fastapi.responses import JSONResponse
import python_opentelemetry_access.proxy as proxy
import python_opentelemetry_access.otlpjson as otlpjson
import python_opentelemetry_access.util as util

from typing import Optional, Annotated, List, Tuple

# from functools import lru_cache
from dataclasses import dataclass

from fastapi import APIRouter, FastAPI, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware

# from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field  # , Base64Bytes

import base64

from datetime import datetime

from python_opentelemetry_access.util.json_api_types import (
    APIErrorResponse,
    APIOKResponseList,
    LinkObject,
    Links,
    Resource as JSONAPIResource,
    get_url_str,
)


@dataclass
class Settings:
    proxy: Optional[proxy.Proxy]


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
settings = Settings(proxy=None)  # type: ignore


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


type APIOKResponse = APIOKResponseList[otlpjson.OTLPJsonSpanCollection.Representation]


class APIResponse(BaseModel):
    results: List[otlpjson.OTLPJsonSpanCollection.Representation]
    next_page_token: Optional[str]


def list_to_dict(values: list[str]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = defaultdict(list)
    for value in values:
        key, value = value.split("=")
        result[key].append(value)
    return result


async def run_query(
    request: Request,
    path: str,
    span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]],
    query_params: QueryParams,
) -> APIOKResponse:
    if settings.proxy is None:
        raise RuntimeError("No proxy initialised")

    new_page_tokens = []
    span_sets = []

    if query_params.page_token is not None:
        page_tokens: List[Optional[proxy.PageToken]] = [
            proxy.PageToken(base64.b64decode(token))
            for token in query_params.page_token.split(".")
        ]
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

    base_url = str(request.base_url)
    query_params_list = [
        (key, value)
        for key, value in request.query_params.multi_items()
        if key != "page_token"
    ]
    link_first = get_url_str(base_url, path, request.path_params, query_params_list)
    link_next = (
        get_url_str(
            base_url,
            path,
            request.path_params,
            query_params_list=query_params_list + [("page_token", next_page_token)],
        )
        if next_page_token is not None
        else None
    )

    return APIOKResponseList[otlpjson.OTLPJsonSpanCollection.Representation](
        data=[
            JSONAPIResource[
                otlpjson.OTLPJsonSpanCollection.Representation
            ].model_construct(id=None, type="resourceSpans", attributes=span_set)
            for span_set in span_sets
        ],
        links=Links(
            self=str(request.url),
            first=link_first,
            next=link_next,
            root=str(request.base_url),
        ),
        meta={"page": {"next_page_token": next_page_token}},
    )


class JSONAPIResponse(JSONResponse):
    media_type = "application/vnd.api+json"


# router = APIRouter(
#     default_response_class=JSONAPIResponse,
#     responses={status.HTTP_422_UNPROCESSABLE_ENTITY: {"model": APIErrorResponse}},
# )


# app. instead of router. because don't want to indicate that could return HTTP error 422
@app.get(
    "/",
    status_code=status.HTTP_200_OK,
    response_model_exclude_unset=True,
    response_class=JSONAPIResponse,
)
async def root(request: Request) -> APIOKResponseList[None]:
    base_url = str(request.base_url)
    return APIOKResponseList[None](
        data=[
            JSONAPIResource[None](
                id="documentation_website",
                type="api_path",
                links={"self": get_url_str(base_url, "/docs")},
            ),
            JSONAPIResource[None](
                id="get_spans",
                type="api_path",
                links={"self": get_url_str(base_url, "/v1/spans")},
            ),
        ],
        links=Links(
            self=base_url,
            describedby=LinkObject(
                title="OpenAPI schema", href=get_url_str(base_url, "/openapi.json")
            ),
        ),
    )


@app.get(
    "/v1/spans",
    response_class=JSONAPIResponse,
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


@app.get(
    "/v1/spans/{trace_id}",
    response_class=JSONAPIResponse,
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


@app.get(
    "/v1/spans/{trace_id}/{span_id}",
    response_class=JSONAPIResponse,
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
