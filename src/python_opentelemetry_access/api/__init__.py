import python_opentelemetry_access.proxy as proxy
import python_opentelemetry_access.otlpjson as otlpjson

from typing import Optional, Annotated, List

# from functools import lru_cache
from dataclasses import dataclass

from fastapi import FastAPI, Query

# from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field  # , Base64Bytes

import base64

from datetime import datetime


@dataclass
class Settings:
    proxy: Optional[proxy.Proxy]


app = FastAPI()
settings = Settings(proxy=None)  # type: ignore


class QueryParams(BaseModel):
    ## Filter parameters
    ## TODO: Expand this with the remaining filter params
    from_time: Optional[datetime] = Field(None)
    to_time: Optional[datetime] = Field(None)

    ## TODO: Projection/verbosity parameters??

    ## Pagination parameters
    ## TODO: (Maybe?) expand this with max_results hint

    ## NOTE: Should be Optional[Base64Bytes], but this makes /docs
    ##       add a file upload field...
    page_token: Optional[str] = Field(None)


class APIResponse(BaseModel):
    results: List[otlpjson.OTLPJsonSpanCollection.Representation]
    next_page_token: Optional[str]


@app.get("/v1/spans")
async def get_spans(query_params: Annotated[QueryParams, Query()]):
    # async def content():
    #     async for spans in settings.proxy.query_spans():
    #         for chunk in spans.to_otlp_json_iter():
    #             yield chunk

    # return StreamingResponse(
    #     content=content(),
    # )
    if settings.proxy is not None:
        return [
            x
            async for x in settings.proxy.query_spans_async(
                from_time=query_params.from_time,
                to_time=query_params.to_time,
                starting_page_token=proxy.PageToken(
                    base64.b64decode(query_params.page_token)
                )
                if query_params.page_token is not None
                else None,
            )
        ]


@app.get("/v1/spans/{trace_id}")
async def get_trace(trace_id: str, query_params: Annotated[QueryParams, Query()]):
    if settings.proxy is not None:
        return [
            x
            async for x in settings.proxy.query_spans_async(
                span_ids=[(trace_id, None)],
                from_time=query_params.from_time,
                to_time=query_params.to_time,
                starting_page_token=proxy.PageToken(
                    base64.b64decode(query_params.page_token)
                )
                if query_params.page_token is not None
                else None,
            )
        ]


@app.get("/v1/spans/{trace_id}/{span_id}")
async def get_span(trace_id, span_id, query_params: Annotated[QueryParams, Query()]):
    if settings.proxy is not None:
        return [
            x
            async for x in settings.proxy.query_spans_async(
                span_ids=[(trace_id, span_id)],
                from_time=query_params.from_time,
                to_time=query_params.to_time,
                starting_page_token=proxy.PageToken(
                    base64.b64decode(query_params.page_token)
                )
                if query_params.page_token is not None
                else None,
            )
        ]
