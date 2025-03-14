from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Any, AsyncIterable, Sequence, Type, assert_never
from opensearchpy import AsyncOpenSearch
from pydantic import BaseModel
import pytest
from opentelemetry import trace
from opentelemetry.util import types
from opentelemetry.trace.span import Span

from python_opentelemetry_access.proxy.opensearch.ss4o import OpenSearchSS40Proxy

tracer = trace.get_tracer("pytest-opentelemetry")


type NestedDict = dict[str, NestedDict | types.AttributeValue]


def flatten_internal(d: NestedDict, name: str) -> dict[str, types.AttributeValue]:
    result: dict[str, types.AttributeValue] = {}
    if name:
        name += "."
    for key, value in d.items():
        cur_name = name + key
        match value:
            case int() | str() | float() | bool() | Sequence():
                result[cur_name] = value
            case dict():
                result |= flatten_internal(value, cur_name)
            case unreachable:
                assert_never(unreachable)
    return result


def flatten(d: NestedDict) -> dict[str, types.AttributeValue]:
    return flatten_internal(d, "")


def set_span_attributes(span: Span, data: BaseModel) -> None:
    span.set_attributes(flatten_internal(data.model_dump(), ""))


async def get_data[T: BaseModel](
    data_type: Type[T],
    span_name: str,
    from_time: datetime | None,
    to_time: datetime | None,
) -> AsyncIterable[T]:
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
    proxy = OpenSearchSS40Proxy(client)
    try:
        async for spanCollection in proxy.query_spans_async(from_time, to_time):
            for resource, scope, span in spanCollection.iter_spans():
                if span.otlp_name == span_name:
                    # TODO: unflatten the data
                    yield data_type.model_validate(span.otlp_attributes)
    finally:
        await client.close()


class Data(BaseModel):
    foo: int
    bar: list[str]


INFO_DUMP_SPAN_NAME = "INFO_DUMP"


def test_generate_data() -> None:
    with tracer.start_as_current_span(INFO_DUMP_SPAN_NAME) as info_dump_span:
        data = Data(foo=5, bar=["a", "bb", "ccc"])
        set_span_attributes(info_dump_span, data)


@pytest.mark.asyncio
async def test_read_data() -> None:
    async for data in get_data(
        data_type=Data,
        span_name=INFO_DUMP_SPAN_NAME,
        from_time=datetime.now() + relativedelta(months=-1),
        to_time=datetime.now(),
    ):
        assert data.foo < 5
