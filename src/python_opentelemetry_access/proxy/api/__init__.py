import python_opentelemetry_access.proxy as proxy
# import python_opentelemetry_access.otlpjson as otlpjson

# from typing import Union
# from functools import lru_cache
from dataclasses import dataclass

from fastapi import FastAPI


@dataclass
class Settings:
    proxy: proxy.Proxy


app = FastAPI()
settings = Settings(proxy=None)  # type: ignore


@app.get("/")
async def read_root():
    yield "Hello"


async def testme():
    for i in range(0, 10):
        yield i


@app.get("/spans")
async def get_spans():
    spans = []
    async for span_collection in settings.proxy.query_spans():
        for resource_spans in span_collection.otlp_resource_spans:
            for scope_spans in resource_spans.otlp_scope_spans:
                for span in scope_spans.otlp_spans:
                    spans.append(span.otlp_span_id)
    return spans


@app.get("/debug")
async def debug():
    span_collections = []
    async for span_collection in settings.proxy.query_spans():
        span_collections.append(span_collection)
    return span_collections[0]
