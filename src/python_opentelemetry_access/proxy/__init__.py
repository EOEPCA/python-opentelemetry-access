import python_opentelemetry_access.base as base
import python_opentelemetry_access.util as util

from dataclasses import dataclass
from abc import ABC, abstractmethod
from collections.abc import AsyncIterable
from typing import List, Optional, Tuple, override
from datetime import datetime


@dataclass
class PageToken:
    token: bytes


class Proxy(ABC):
    @abstractmethod
    async def query_spans_page(
        self,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]] = None,
        resource_attributes: Optional[dict[str, list[str]]] = None,
        scope_attributes: Optional[dict[str, list[str]]] = None,
        span_attributes: Optional[dict[str, list[str]]] = None,
        page_token: Optional[PageToken] = None,
    ) -> AsyncIterable[base.SpanCollection | PageToken]:
        # A trick to make the type of the function what I want
        # Why yield inside function body effects the type of the function is explained in
        # https://mypy.readthedocs.io/en/stable/more_types.html#asynchronous-iterators
        if False:
            yield

    async def query_spans_async(
        self,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]] = None,
        resource_attributes: Optional[dict[str, list[str]]] = None,
        scope_attributes: Optional[dict[str, list[str]]] = None,
        span_attributes: Optional[dict[str, list[str]]] = None,
        starting_page_token: Optional[PageToken] = None,
    ) -> AsyncIterable[base.SpanCollection]:
        async for spans_or_page_token in self.query_spans_page(
            from_time,
            to_time,
            span_ids,
            resource_attributes,
            scope_attributes,
            span_attributes,
            page_token=starting_page_token,
        ):
            if isinstance(spans_or_page_token, PageToken):
                async for spans in self.query_spans_async(
                    from_time,
                    to_time,
                    span_ids,
                    resource_attributes,
                    scope_attributes,
                    span_attributes,
                    starting_page_token=spans_or_page_token,
                ):
                    yield spans
            else:
                yield spans_or_page_token


def _match_span(
    span: base.ReifiedSpan,
    from_time: Optional[datetime] = None,
    to_time: Optional[datetime] = None,
    span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]] = None,
    span_attributes: Optional[dict[str, list[str]]] = None,
) -> bool:
    if (
        to_time is not None
        and span.start_time_unix_nano > to_time.timestamp() * 1000000000
    ):
        return False

    if (
        from_time is not None
        and span.end_time_unix_nano < from_time.timestamp() * 1000000000
    ):
        return False

    if (
        span_ids is not None
        and (span.trace_id, None) not in span_ids
        and (span.trace_id, span.span_id) not in span_ids
    ):
        return False

    return util.match_attributes(
        actual_attributes=span.attributes, expected_attributes=span_attributes
    )


def _filter_scope_span_collection(
    spans: base.ReifiedScopeSpanCollection,
    from_time: Optional[datetime] = None,
    to_time: Optional[datetime] = None,
    span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]] = None,
    span_attributes: Optional[dict[str, list[str]]] = None,
) -> base.ReifiedScopeSpanCollection:
    spans.spans = [
        span
        for span in spans.spans
        if _match_span(span, from_time, to_time, span_ids, span_attributes)
    ]

    return spans


def _filter_resource_span_collection(
    spans: base.ReifiedResourceSpanCollection,
    from_time: Optional[datetime] = None,
    to_time: Optional[datetime] = None,
    span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]] = None,
    scope_attributes: Optional[dict[str, list[str]]] = None,
    span_attributes: Optional[dict[str, list[str]]] = None,
) -> base.ReifiedResourceSpanCollection:
    spans.scope_spans = [
        _filter_scope_span_collection(
            inner_spans, from_time, to_time, span_ids, span_attributes
        )
        for inner_spans in spans.scope_spans
        if util.match_attributes(
            actual_attributes=inner_spans.scope.attributes,
            expected_attributes=scope_attributes,
        )
    ]

    spans.scope_spans = [
        inner_spans for inner_spans in spans.scope_spans if inner_spans.spans
    ]

    return spans


def _filter_span_collection(
    spans: base.ReifiedSpanCollection,
    from_time: Optional[datetime] = None,
    to_time: Optional[datetime] = None,
    span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]] = None,
    resource_attributes: Optional[dict[str, list[str]]] = None,
    scope_attributes: Optional[dict[str, list[str]]] = None,
    span_attributes: Optional[dict[str, list[str]]] = None,
) -> base.ReifiedSpanCollection:
    spans.resource_spans = [
        _filter_resource_span_collection(
            inner_spans,
            from_time,
            to_time,
            span_ids,
            scope_attributes,
            span_attributes,
        )
        for inner_spans in spans.resource_spans
        if util.match_attributes(
            inner_spans.resource.attributes, expected_attributes=resource_attributes
        )
    ]

    spans.resource_spans = [
        inner_spans for inner_spans in spans.resource_spans if inner_spans.scope_spans
    ]

    return spans


class MockProxy(Proxy):
    def __init__(self, all_spans: base.SpanCollection):
        self._all_spans = all_spans.to_reified()

    @override
    async def query_spans_page(
        self,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]] = None,
        resource_attributes: Optional[dict[str, list[str]]] = None,
        scope_attributes: Optional[dict[str, list[str]]] = None,
        span_attributes: Optional[dict[str, list[str]]] = None,
        page_token: Optional[PageToken] = None,
    ) -> AsyncIterable[base.SpanCollection | PageToken]:
        if page_token is not None:
            raise RuntimeError("Unexpected page token")

        yield _filter_span_collection(
            self._all_spans.to_reified(),
            from_time,
            to_time,
            span_ids,
            resource_attributes,
            scope_attributes,
            span_attributes,
        )
