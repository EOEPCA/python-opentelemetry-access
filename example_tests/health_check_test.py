import asyncio
from datetime import datetime, timedelta
from typing import Any, AsyncIterable, Coroutine, Iterable
from opensearchpy import AsyncOpenSearch
import pytest
from opentelemetry import trace
from opentelemetry.util import types

from python_opentelemetry_access import otlpjson, proxy
from python_opentelemetry_access.base import Span
from python_opentelemetry_access.proxy.opensearch.ss4o import OpenSearchSS40Proxy


type NestedDict = dict[str, NestedDict | types.AttributeValue]


def _get_fields_attr_name(data_name: str) -> str:
    return f"data_{data_name}_fields"


# THESE TWO functions should probably be the new API, at least for the simplest cases
# THOUGH with this you don't get span length. Might include span length in the result?
def save_data(data_name: str, data: dict[str, types.AttributeValue]) -> None:
    cur_span = trace.get_current_span()
    cur_span.set_attribute(_get_fields_attr_name(data_name), list(data.keys()))
    trace.get_current_span().set_attributes(data)


async def load_data_from_name_async(
    proxy: proxy.Proxy, data_name: str, max_data_age: timedelta
) -> AsyncIterable[dict[str, types.AttributeValue]]:
    try:
        now = datetime.now()
        fields_attr_name = _get_fields_attr_name(data_name)
        async for spanCollection in proxy.query_spans_async(
            from_time=now - max_data_age,
            to_time=now,
            span_attributes={fields_attr_name: None},
        ):
            for _resource, _scope, span in spanCollection.iter_spans():
                # fields = [
                #     expect_str(field)
                #     for field in expect_list(span.otlp_attributes[fields_attr_name])
                # ]
                fields = span.otlp_attributes[fields_attr_name]
                yield {field: span.otlp_attributes[field] for field in fields}  # type: ignore
    finally:
        await proxy.aclose()


async def load_span_from_fields_async(
    proxy: proxy.Proxy, data_fields: list[str], max_data_age: timedelta
) -> AsyncIterable[Span]:
    try:
        now = datetime.now()
        async for spanCollection in proxy.query_spans_async(
            from_time=now - max_data_age,
            to_time=now,
            span_attributes={field: None for field in data_fields},
        ):
            for _resource, _scope, span in spanCollection.iter_spans():
                yield span
    finally:
        await proxy.aclose()


async def load_span_from_span_name_async(
    proxy: proxy.Proxy, span_name: str, max_data_age: timedelta
) -> AsyncIterable[Span]:
    try:
        now = datetime.now()
        async for spanCollection in proxy.query_spans_async(
            from_time=now - max_data_age, to_time=now, span_name=span_name
        ):
            for _resource, _scope, span in spanCollection.iter_spans():
                yield span
    finally:
        await proxy.aclose()


async def async_list[T](
    async_iterable: AsyncIterable[T],
) -> Iterable[T]:
    # TODO: would be nice to return the values as they become available, not accumulate them all first
    results: list[T] = []
    async for data in async_iterable:
        results.append(data)
    return results


def async_to_sync_iterable[T](
    async_iterable: AsyncIterable[T],
) -> Iterable[T]:
    return asyncio.run(async_list(async_iterable))


def load_data_sync(
    proxy: proxy.Proxy, data_name: str, max_data_age: timedelta
) -> Iterable[dict[str, types.AttributeValue]]:
    return async_to_sync_iterable(
        load_data_from_name_async(proxy, data_name, max_data_age)
    )
    # return asyncio.run(_load_data_internal(proxy, data_name, max_data_age))


def get_mock_proxy(file: str) -> proxy.Proxy:
    with open(file, "r") as f:
        return proxy.MockProxy(otlpjson.load(f))


def get_opensearch_proxy() -> proxy.Proxy:
    opensearch_params: dict[str, Any] = {}

    # if osuser is not None and ospass is not None:
    #     opensearch_params["http_auth"] = (osuser, ospass)
    opensearch_params["ca_certs"] = "tmp/tmp_certs/ca.crt"
    opensearch_params["client_cert"] = "tmp/tmp_certs/tls.crt"
    opensearch_params["client_key"] = "tmp/tmp_certs/tls.key"
    opensearch_params.update({"verify_certs": True, "ssl_show_warn": True})

    client = AsyncOpenSearch(
        hosts=[{"host": "opensearch-cluster-master-headless", "port": 9200}],
        use_ssl=True,
        **opensearch_params,
    )
    return OpenSearchSS40Proxy(client)


INFO_DUMP_SPAN_NAME = "INFO_DUMP"


# def test_bla() -> None:
#     with tracer.start_as_current_span("HELLO_SPAN") as span:
#         span.set_attribute("hello", [1, 2, 3])


def test_generate_data() -> None:
    save_data(data_name="my_data", data={"results": [1.0, 2.0, 2.0]})


def test_get_requests_sync() -> None:
    sum_sum = 0.0
    sum_count = 0
    for data in load_data_sync(
        proxy=get_mock_proxy("example_tests/test_spans.json"),
        data_name="my_data",
        max_data_age=timedelta(weeks=4),
    ):
        sum_sum += sum(data["results"])
        sum_count += 1
    assert sum_sum / sum_count < 2.0


@pytest.mark.asyncio
async def test_get_requests_async() -> None:
    sum_sum = 0.0
    sum_count = 0
    async for data in load_data_from_name_async(
        proxy=get_mock_proxy("example_tests/test_spans.json"),
        data_name="my_data",
        max_data_age=timedelta(weeks=4),
    ):
        sum_sum += sum(data["results"])
        sum_count += 1
    assert sum_sum / sum_count < 2.0


# # TODO: make this function unnecessary. proxy.query_spans_async should be able to do all the necessary filtering by itself
# async def get_spans_with_data(
#     proxy: proxy.Proxy,
#     required_attributes: list[str],
#     from_time: datetime | None,
#     to_time: datetime | None,
# ) -> AsyncIterable[base.Span]:
#     async for spanCollection in proxy.query_spans_async(from_time, to_time):
#         for _resource, _scope, span in spanCollection.iter_spans():
#             # TODO: filter by these
#             if all(attr in span.otlp_attributes for attr in required_attributes):
#                 yield span


# @pytest.mark.asyncio
# async def test_get_requests() -> None:
#     proxy = get_opensearch_proxy()
#     try:
#         async for span in get_spans_with_data(
#             proxy=proxy,
#             required_attributes=["http.status_code", "http.url", "http.method"],
#             from_time=datetime.now() + relativedelta(months=-1),
#             to_time=datetime.now(),
#         ):
#             assert span.otlp_attributes["http.status_code"] == 200
#     finally:
#         await proxy.aclose()
