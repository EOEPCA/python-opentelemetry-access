import typing
import requests
from collections.abc import AsyncIterable
from datetime import datetime
from urllib.parse import urljoin

from python_opentelemetry_access import base as base, proxy as proxy
from python_opentelemetry_access.otlpjson import OTLPJsonSpanCollection


def _query_obj(
    from_time: typing.Optional[datetime] = None,
    to_time: typing.Optional[datetime] = None,
    resource_attributes: typing.Optional[dict[str, list[str]]] = None,
    scope_attributes: typing.Optional[dict[str, list[str]]] = None,
    span_attributes: typing.Optional[dict[str, list[str]]] = None,
) -> dict[str, str]:
    params = {}

    if from_time is not None:
        params["from_time"] = from_time.isoformat()
    if to_time is not None:
        params["to_time"] = to_time.isoformat()
    if resource_attributes is not None and len(resource_attributes) > 0:
        raise NotImplementedError("")
        # params["resource_attributes"] = [
        #
        # ]
    if scope_attributes is not None and len(scope_attributes) > 0:
        raise NotImplementedError("")
        pass
    if span_attributes is not None and len(span_attributes) > 0:
        raise NotImplementedError("")
        pass

    return params


_REST_PROXY_INDEX_BYTES = 4
_REST_PROXY_INDEX_ENDIAN: typing.Literal["little", "big"] = "little"
_REST_PROXY_TOKEN_ENCODING = "ascii"


def _decode_rest_proxy_token(
    raw_token: typing.Optional[bytes],
) -> tuple[int, str | None]:
    if raw_token is None:
        return (0, None)
    else:
        if len(raw_token) <= _REST_PROXY_INDEX_BYTES:
            return (
                int.from_bytes(
                    raw_token, byteorder=_REST_PROXY_INDEX_ENDIAN, signed=False
                ),
                None,
            )
        else:
            return (
                int.from_bytes(raw_token[:_REST_PROXY_INDEX_BYTES], signed=False),
                raw_token[_REST_PROXY_INDEX_BYTES:].decode(_REST_PROXY_TOKEN_ENCODING),
            )


def _encode_rest_proxy_token(skip_to_idx: int, other_token: str | None) -> bytes:
    index_bytes = skip_to_idx.to_bytes(
        _REST_PROXY_INDEX_BYTES, byteorder=_REST_PROXY_INDEX_ENDIAN, signed=False
    )

    if other_token is None:
        return index_bytes

    return index_bytes + other_token.encode(_REST_PROXY_TOKEN_ENCODING)


class RESTProxy(proxy.Proxy):
    def __init__(
        self, base_url: str, session: typing.Optional[requests.Session] = None
    ):
        if session is None:
            session = requests.Session()

        self._base_url = base_url
        self._session = session

    def _span_url(
        self, trace_id: typing.Optional[str], span_id: typing.Optional[str]
    ) -> str:
        if trace_id is None:
            return urljoin(self._base_url, "/v1/spans")
        else:
            if span_id is None:
                return urljoin(self._base_url, f"/v1/spans/{trace_id}")
            else:
                return urljoin(self._base_url, f"/v1/spans/{trace_id}/{span_id}")

    @typing.override
    async def query_spans_page(
        self,
        from_time: typing.Optional[datetime] = None,
        to_time: typing.Optional[datetime] = None,
        span_ids: typing.Optional[
            list[tuple[typing.Optional[str], typing.Optional[str]]]
        ] = None,
        resource_attributes: typing.Optional[dict[str, list[str]]] = None,
        scope_attributes: typing.Optional[dict[str, list[str]]] = None,
        span_attributes: typing.Optional[dict[str, list[str]]] = None,
        page_token: typing.Optional[proxy.PageToken] = None,
    ) -> AsyncIterable[base.SpanCollection | proxy.PageToken]:
        skip_to_idx, remote_token = _decode_rest_proxy_token(
            page_token.token if page_token is not None else None
        )

        if span_ids is None:
            span_ids = [(None, None)]

        params = _query_obj(
            from_time=from_time,
            to_time=to_time,
            resource_attributes=resource_attributes,
            scope_attributes=scope_attributes,
            span_attributes=span_attributes,
        )

        if remote_token is not None:
            params["page_token"] = remote_token

        (trace_id, span_id) = span_ids[skip_to_idx]

        # If our data always corresponed to the type it says id does,
        # i.e. APIOKResponseList[otlpjson.OTLPJsonSpanCollection.Representation, ResponseMeta]
        # then I would use the following code
        # result = APIOKResponseList[
        #     otlpjson.OTLPJsonSpanCollection.Representation, ResponseMeta
        # ].model_validate(
        #     self._session.get(self._span_url(trace_id, span_id), params=params).json()
        # )
        # for spans in result.data:
        #     yield OTLPJsonSpanCollection(spans.attributes)
        # next_page_token = result.meta.page.next_page_token

        result = self._session.get(
            self._span_url(trace_id, span_id), params=params
        ).json()
        for spans in result["data"]:
            yield OTLPJsonSpanCollection(spans["attributes"])
        next_page_token = result["meta"]["page"]["next_page_token"]
        if next_page_token is not None:
            yield proxy.PageToken(
                _encode_rest_proxy_token(skip_to_idx, next_page_token)
            )
        elif skip_to_idx < len(span_ids):
            yield proxy.PageToken(_encode_rest_proxy_token(skip_to_idx + 1, None))

    @typing.override
    async def query_spans_async(
        self,
        from_time: typing.Optional[datetime] = None,
        to_time: typing.Optional[datetime] = None,
        span_ids: typing.Optional[
            list[tuple[typing.Optional[str], typing.Optional[str]]]
        ] = None,
        resource_attributes: typing.Optional[dict[str, list[str]]] = None,
        scope_attributes: typing.Optional[dict[str, list[str]]] = None,
        span_attributes: typing.Optional[dict[str, list[str]]] = None,
        starting_page_token: typing.Optional[proxy.PageToken] = None,
    ) -> AsyncIterable[base.SpanCollection]:
        skip_to_idx, remote_token = _decode_rest_proxy_token(
            starting_page_token.token if starting_page_token is not None else None
        )

        if span_ids is None:
            span_ids = [(None, None)]

        params = _query_obj(
            from_time=from_time,
            to_time=to_time,
            resource_attributes=resource_attributes,
            scope_attributes=scope_attributes,
            span_attributes=span_attributes,
        )

        for idx, (trace_id, span_id) in enumerate(span_ids):
            if idx < skip_to_idx:
                pass

            if remote_token is not None:
                params["page_token"] = remote_token

            next_request: requests.PreparedRequest | None = requests.Request(
                "GET", url=self._span_url(trace_id, span_id), params=params
            ).prepare()

            if remote_token is not None:
                del params["page_token"]
                remote_token = None

            while next_request is not None:
                result = self._session.send(next_request).json()
                yield result["data"]

                next_url = result["links"]["next"]
                next_request = (
                    requests.Request("GET", next_url).prepare()
                    if next_url is not None
                    else None
                )
