from dataclasses import dataclass
from abc import ABC, abstractmethod
from collections.abc import AsyncIterable
from typing import Iterable, List, Optional, Tuple, override
from datetime import datetime, timedelta

import python_opentelemetry_access.base as base
import python_opentelemetry_access.util as util


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
        resource_attributes: Optional[util.AttributesFilter] = None,
        scope_attributes: Optional[util.AttributesFilter] = None,
        span_attributes: Optional[util.AttributesFilter] = None,
        span_name: Optional[str] = None,
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
        resource_attributes: Optional[util.AttributesFilter] = None,
        scope_attributes: Optional[util.AttributesFilter] = None,
        span_attributes: Optional[util.AttributesFilter] = None,
        span_name: Optional[str] = None,
        starting_page_token: Optional[PageToken] = None,
    ) -> AsyncIterable[base.SpanCollection]:
        async for spans_or_page_token in self.query_spans_page(
            from_time,
            to_time,
            span_ids,
            resource_attributes,
            scope_attributes,
            span_attributes,
            span_name,
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
                    span_name,
                    starting_page_token=spans_or_page_token,
                ):
                    yield spans
            else:
                yield spans_or_page_token

    async def load_span_data_async(
        self,
        span_name: Optional[str],
        span_attributes: Optional[util.AttributesFilter],
        max_data_age: timedelta,
    ) -> AsyncIterable[base.ReifiedSpan]:
        now = datetime.now()
        async for spanCollection in self.query_spans_async(
            from_time=now - max_data_age,
            to_time=now,
            span_attributes=span_attributes,
            span_name=span_name,
        ):
            for _resource, _scope, span in spanCollection.iter_spans():
                yield span.to_reified()

    def load_span_data_sync(
        self,
        span_name: str | None,
        span_attributes: util.AttributesFilter | None,
        max_data_age: timedelta,
    ) -> Iterable[base.ReifiedSpan]:
        return util.async_to_sync_iterable(
            self.load_span_data_async(span_name, span_attributes, max_data_age)
        )

    # Close connections, release resources and such
    # aclose is the standard name such methods when they are asynchronous
    @abstractmethod
    async def aclose(self) -> None:
        pass


def _match_span(
    span: base.ReifiedSpan,
    from_time: Optional[datetime],
    to_time: Optional[datetime],
    span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]],
    span_attributes: Optional[util.AttributesFilter],
    span_name: Optional[str],
) -> bool:
    if span_name is not None and span.name != span_name:
        return False

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
    from_time: Optional[datetime],
    to_time: Optional[datetime],
    span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]],
    span_attributes: Optional[util.AttributesFilter],
    span_name: Optional[str],
) -> base.ReifiedScopeSpanCollection:
    spans.spans = [
        span
        for span in spans.spans
        if _match_span(span, from_time, to_time, span_ids, span_attributes, span_name)
    ]

    return spans


def _filter_resource_span_collection(
    spans: base.ReifiedResourceSpanCollection,
    from_time: Optional[datetime],
    to_time: Optional[datetime],
    span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]],
    scope_attributes: Optional[util.AttributesFilter],
    span_attributes: Optional[util.AttributesFilter],
    span_name: Optional[str],
) -> base.ReifiedResourceSpanCollection:
    spans.scope_spans = [
        _filter_scope_span_collection(
            inner_spans, from_time, to_time, span_ids, span_attributes, span_name
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
    from_time: Optional[datetime],
    to_time: Optional[datetime],
    span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]],
    resource_attributes: Optional[util.AttributesFilter],
    scope_attributes: Optional[util.AttributesFilter],
    span_attributes: Optional[util.AttributesFilter],
    span_name: Optional[str],
) -> base.ReifiedSpanCollection:
    spans.resource_spans = [
        _filter_resource_span_collection(
            inner_spans,
            from_time,
            to_time,
            span_ids,
            scope_attributes,
            span_attributes,
            span_name,
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
        resource_attributes: Optional[util.AttributesFilter] = None,
        scope_attributes: Optional[util.AttributesFilter] = None,
        span_attributes: Optional[util.AttributesFilter] = None,
        span_name: Optional[str] = None,
        page_token: Optional[PageToken] = None,
    ) -> AsyncIterable[base.SpanCollection | PageToken]:
        if page_token is not None:
            raise util.InvalidPageTokenException.create()
            # raise APIException(
            #     Error(
            #         status="400",
            #         code="UnexpectedPageToken",
            #         title="Unexpected Page Token",
            #         detail="Mock Proxy doesn't support page token (as it never issues next page tokens)",
            #     )
            # )

        yield _filter_span_collection(
            self._all_spans.to_reified(),
            from_time,
            to_time,
            span_ids,
            resource_attributes,
            scope_attributes,
            span_attributes,
            span_name,
        )

    @override
    async def aclose(self) -> None:
        pass
