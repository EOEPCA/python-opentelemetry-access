from datetime import timedelta
from typing import Any, AsyncIterable, Iterable
from opensearchpy import AsyncOpenSearch
import pytest
from opentelemetry import trace
from opentelemetry.util import types

from python_opentelemetry_access import otlpjson
from python_opentelemetry_access import util
from python_opentelemetry_access.proxy import MockProxy, Proxy
from python_opentelemetry_access.proxy.opensearch.ss4o import OpenSearchSS40Proxy


def _get_fields_attr_name(data_name: str) -> str:
    return f"data_{data_name}_fields"


async def load_data_from_name_async(
    proxy: Proxy, data_name: str, max_data_age: timedelta
) -> AsyncIterable[util.CheckData]:
    fields_attr_name = _get_fields_attr_name(data_name)
    async for data in proxy.load_span_data_async(
        span_name=None,
        span_attributes={fields_attr_name: None},
        max_data_age=max_data_age,
    ):
        fields: list[str] = data.attributes[fields_attr_name]
        yield util.CheckData(
            span_duration=data.span_duration,
            attributes={field: data.attributes[field] for field in fields},
        )


def load_data_from_name_sync(
    proxy: Proxy, data_name: str, max_data_age: timedelta
) -> Iterable[util.CheckData]:
    return util.async_to_sync_iterable(
        load_data_from_name_async(proxy, data_name, max_data_age)
    )


def save_data(data_name: str, data: dict[str, types.AttributeValue]) -> None:
    cur_span = trace.get_current_span()
    cur_span.set_attribute(_get_fields_attr_name(data_name), list(data.keys()))
    cur_span.set_attributes(data)


def get_mock_proxy(file: str) -> Proxy:
    with open(file, "r") as f:
        return MockProxy(otlpjson.load(f))


def get_opensearch_proxy() -> Proxy:
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


# USER DEFINED CODE START


def test_requests_duration() -> None:
    duration_sum = timedelta()
    duration_count = 0
    for data in get_opensearch_proxy().load_span_data_sync(
        span_name="GET",
        span_attributes={
            "http.url": ["https://openeo.dataspace.copernicus.eu/openeo/1.2"]
        },
        max_data_age=timedelta(weeks=4),
    ):
        duration_sum += data.span_duration
        duration_count += 1
    assert duration_count >= 1
    assert duration_sum / duration_count < timedelta(milliseconds=100)


def test_generate_data() -> None:
    save_data(data_name="my_data", data={"results": [1.0, 2.0, 2.0]})


def test_get_requests_sync() -> None:
    sum_sum = 0.0
    sum_count = 0
    for data in load_data_from_name_sync(
        proxy=get_mock_proxy("example_tests/test_spans.json"),
        data_name="my_data",
        max_data_age=timedelta(weeks=4),
    ):
        sum_sum += sum(data.attributes["results"])
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
        sum_sum += sum(data.attributes["results"])
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
