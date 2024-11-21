import python_opentelemetry_access.proxy as proxy
# import python_opentelemetry_access.otlpjson as otlpjson

# from typing import Union
# from functools import lru_cache
from dataclasses import dataclass

from fastapi import FastAPI
# from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

@dataclass
class Settings:
    proxy: proxy.Proxy


app = FastAPI()
settings = Settings(proxy=None)  # type: ignore

class FilterParams(BaseModel):
    limit: int = Field(100, gt=0, le=100)
    offset: int = Field(0, ge=0)
    tags: list[str] = []

@app.get("v1/")
async def read_root():
    yield "Hello"


@app.get("v1/spans")
async def get_spans():
    # async def content():
    #     async for spans in settings.proxy.query_spans():
    #         for chunk in spans.to_otlp_json_iter():
    #             yield chunk

    # return StreamingResponse(
    #     content=content(),
    # )
    return [x async for x in settings.proxy.query_spans()]


@app.get("v1/spans/{trace_id}")
async def get_trace(trace_id):
    return settings.proxy.query_spans(span_ids=[(trace_id, None)])


@app.get("v1/spans/{trace_id}/{span_id}")
async def get_span(trace_id, span_id):
    return settings.proxy.query_spans(span_ids=[(trace_id, span_id)])
