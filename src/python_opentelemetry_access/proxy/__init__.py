import python_opentelemetry_access.base as base
import python_opentelemetry_access.util as util

from abc import ABC, abstractmethod
from collections.abc import AsyncIterable
from typing import List, Optional, Tuple
from datetime import datetime


class Proxy(ABC):
    @abstractmethod
    async def query_spans(
        self,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        span_ids: Optional[List[Tuple[str, str]]] = None,
        resource_attributes: Optional[dict] = None,
        scope_attributes: Optional[dict] = None,
        span_attributes: Optional[dict] = None,
    ) -> AsyncIterable[base.SpanCollection]:
        pass


def _match_span(
    span: base.ReifiedSpan,
    from_time: Optional[datetime] = None,
    to_time: Optional[datetime] = None,
    span_ids: Optional[List[Tuple[str, str]]] = None,
    span_attributes: Optional[dict] = None,
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

    if span_attributes is not None:
        span_attributes_flat = util.normalise_attributes_shallow(span_attributes)
        for k, v in util.normalise_attributes_shallow_iter(
            util.iter_jsonlike_dict(span.attributes)
        ):
            if k in span_attributes_flat and v not in util.expect_list(
                span_attributes_flat[k]
            ):
                return False

    return True


def _filter_scope_span_collection(
    spans: base.ReifiedScopeSpanCollection,
    from_time: Optional[datetime] = None,
    to_time: Optional[datetime] = None,
    span_ids: Optional[List[Tuple[str, str]]] = None,
    span_attributes: Optional[dict] = None,
) -> base.ReifiedScopeSpanCollection:
    spans.spans = [
        span
        for span in spans.spans
        if _match_span(span, from_time, to_time, span_ids, span_attributes)
    ]

    return spans


def _match_scope(
    scope: base.ReifiedInstrumentationScope, scope_attributes: Optional[dict] = None
) -> bool:
    if scope_attributes is None:
        return True
    else:
        scope_attributes_flat = util.normalise_attributes_shallow(scope_attributes)
        return all(
            (
                (k not in scope_attributes_flat)
                or (v in util.expect_list(scope_attributes_flat[k]))
                for k, v in util.normalise_attributes_shallow_iter(
                    util.iter_jsonlike_dict(scope.attributes)
                )
            )
        )


def _filter_resource_span_collection(
    spans: base.ReifiedResourceSpanCollection,
    from_time: Optional[datetime] = None,
    to_time: Optional[datetime] = None,
    span_ids: Optional[List[Tuple[str, str]]] = None,
    scope_attributes: Optional[dict] = None,
    span_attributes: Optional[dict] = None,
) -> base.ReifiedResourceSpanCollection:
    spans.scope_spans = [
        _filter_scope_span_collection(
            inner_spans, from_time, to_time, span_ids, span_attributes
        )
        for inner_spans in spans.scope_spans
        if _match_scope(inner_spans.scope, scope_attributes)
    ]

    return spans


def _match_resource(
    resource: base.ReifiedResource, resource_attributes: Optional[dict] = None
) -> bool:
    if resource_attributes is None:
        return True
    else:
        resource_attributes_flat = util.normalise_attributes_shallow(
            resource_attributes
        )
        return all(
            (
                (k not in resource_attributes_flat)
                or (v in util.expect_list(resource_attributes_flat[k]))
                for k, v in util.normalise_attributes_shallow_iter(
                    util.iter_jsonlike_dict(resource.attributes)
                )
            )
        )


def _filter_span_collection(
    spans: base.ReifiedSpanCollection,
    from_time: Optional[datetime] = None,
    to_time: Optional[datetime] = None,
    span_ids: Optional[List[Tuple[str, str]]] = None,
    resource_attributes: Optional[dict] = None,
    scope_attributes: Optional[dict] = None,
    span_attributes: Optional[dict] = None,
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
        if _match_resource(inner_spans.resource, resource_attributes)
    ]

    return spans


class MockProxy:
    def __init__(self, all_spans: base.SpanCollection):
        self._all_spans = all_spans.to_reified()

    async def query_spans(
        self,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        span_ids: Optional[List[Tuple[str, str]]] = None,
        resource_attributes: Optional[dict] = None,
        scope_attributes: Optional[dict] = None,
        span_attributes: Optional[dict] = None,
    ) -> AsyncIterable[base.SpanCollection]:
        yield _filter_span_collection(
            self._all_spans.to_reified(),
            from_time,
            to_time,
            span_ids,
            resource_attributes,
            scope_attributes,
            span_attributes,
        )
