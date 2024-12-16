import python_opentelemetry_access.proxy as proxy
import python_opentelemetry_access.otlpjson as otlpjson
import python_opentelemetry_access.util as util

from typing import Optional, Annotated, List, Tuple, Dict

# from functools import lru_cache
from dataclasses import dataclass

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field  # , Base64Bytes

import base64

from datetime import datetime


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

    span_attributes: Dict[str, str | int | bool] = {}
    resource_attributes: Dict[str, str | int | bool] = {}
    instrumentation_attributes: Dict[str, str | int | bool] = {}

    ## TODO: Projection/verbosity parameters??

    ## Pagination parameters
    ## TODO: (Maybe?) expand this with max_results hint

    ## NOTE: Should be Optional[Base64Bytes], but this makes /docs
    ##       add a file upload field...
    page_token: Optional[str] = Field(None)


class APIResponse(BaseModel):
    results: List[otlpjson.OTLPJsonSpanCollection.Representation]
    next_page_token: Optional[str]


async def run_query(
    span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]],
    query_params: QueryParams,
) -> APIResponse:
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

    for page_token in page_tokens:
        async for res in settings.proxy.query_spans_page(
            span_ids=span_ids,
            from_time=query_params.from_time,
            to_time=query_params.to_time,
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

    # Disable validation for now
    return APIResponse.construct(
        None, results=span_sets, next_page_token=next_page_token
    )


@app.get("/v1/spans")
async def get_spans(query_params: Annotated[QueryParams, Query()]) -> APIResponse:
    return await run_query(span_ids=None, query_params=query_params)


@app.get("/v1/spans/{trace_id}")
async def get_trace(trace_id: str, query_params: Annotated[QueryParams, Query()]) -> APIResponse:
    return await run_query(span_ids=[(trace_id, None)], query_params=query_params)


@app.get("/v1/spans/{trace_id}/{span_id}")
async def get_span(trace_id: str, span_id: str, query_params: Annotated[QueryParams, Query()]) -> APIResponse:
    return await run_query(span_ids=[(trace_id, span_id)], query_params=query_params)
