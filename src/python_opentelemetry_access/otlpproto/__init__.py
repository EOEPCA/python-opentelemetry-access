from collections.abc import Iterator
from typing import Optional, BinaryIO, override
import binascii

from .. import base
from .. import util

import opentelemetry_betterproto.opentelemetry.proto.common.v1 as common
import opentelemetry_betterproto.opentelemetry.proto.trace.v1 as trace
import opentelemetry_betterproto.opentelemetry.proto.collector.trace.v1 as trace_collector
import opentelemetry_betterproto.opentelemetry.proto.resource.v1 as resource


def _un_anyvalue(any_val: common.AnyValue) -> util.JSONLikeIter:
    match any_val:
        case common.AnyValue(string_value=str_val):
            return str_val
        case common.AnyValue(bool_value=bool_value):
            return bool_value
        case common.AnyValue(int_value=int_value):
            return int_value
        case common.AnyValue(double_value=double_value):
            return double_value
        case common.AnyValue(array_value=array_value):
            return util.JSONLikeListIter((_un_anyvalue(x) for x in array_value.values))
        case common.AnyValue(kvlist_value=kvlist_value):
            return util.JSONLikeDictIter(
                ((kv.key, _un_anyvalue(kv.value)) for kv in kvlist_value.values)
            )
    raise NotImplementedError()


class OTLPProtoSpanEvent(base.SpanEvent):
    _proto: trace.SpanEvent

    def __init__(self, proto: trace.SpanEvent):
        self._proto = proto

    @property
    @override
    def otlp_time_unix_nano(self) -> int:
        return self._proto.time_unix_nano

    @property
    @override
    def otlp_name(self) -> str:
        return self._proto.name

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return util.JSONLikeDictIter(
            ((x.key, _un_anyvalue(x.value)) for x in self._proto.attributes)
        )

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self._proto.dropped_attributes_count


class OTLPProtoStatus(base.Status):
    _proto: trace.Status

    def __init__(self, proto: trace.Status):
        self._proto = proto

    @property
    @override
    def otlp_message(self) -> Optional[str]:
        return self._proto.message if self._proto.message != "" else None

    @property
    @override
    def otlp_code(self) -> int:
        return self._proto.code


class OTLPProtoSpanKind(base.SpanKind):
    _proto: trace.SpanSpanKind

    def __init__(self, proto: trace.SpanSpanKind):
        self._proto = proto

    @property
    @override
    def otlp_kind_code(self) -> int:
        return int(self._proto)


class OTLPProtoSpanLink(base.SpanLink):
    _proto: trace.SpanLink

    def __init__(self, proto: trace.SpanLink):
        self._proto = proto

    @property
    @override
    def otlp_trace_id(self) -> str:
        return binascii.b2a_hex(self._proto.trace_id).decode("ascii")

    @property
    @override
    def otlp_span_id(self) -> str:
        return binascii.b2a_hex(self._proto.span_id).decode("ascii")

    @property
    @override
    def otlp_state(self) -> str:
        return self._proto.trace_state

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return util.JSONLikeDictIter(
            ((x.key, _un_anyvalue(x.value)) for x in self._proto.attributes)
        )

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self._proto.dropped_attributes_count

    @property
    @override
    def otlp_flags(self) -> int:
        return self._proto.flags


class OTLPProtoSpan(base.Span):
    _proto: trace.Span

    def __init__(self, proto: trace.Span):
        self._proto = proto

    @property
    @override
    def otlp_trace_id(self) -> str:
        return binascii.b2a_hex(self._proto.trace_id).decode("ascii")

    @property
    @override
    def otlp_span_id(self) -> str:
        return binascii.b2a_hex(self._proto.span_id).decode("ascii")

    @property
    @override
    def otlp_trace_state(self) -> Optional[str]:
        return self._proto.trace_state if self._proto.trace_state != "" else None

    @property
    @override
    def otlp_parent_span_id(self) -> str:
        return binascii.b2a_hex(self._proto.parent_span_id).decode("ascii")

    @property
    @override
    def otlp_flags(self) -> int:
        return self._proto.flags

    @property
    @override
    def otlp_name(self) -> str:
        return self._proto.name

    @property
    @override
    def otlp_kind(self) -> OTLPProtoSpanKind:
        return OTLPProtoSpanKind(self._proto.kind)

    @property
    @override
    def otlp_start_time_unix_nano(self) -> int:
        return self._proto.start_time_unix_nano

    @property
    @override
    def otlp_end_time_unix_nano(self) -> int:
        return self._proto.end_time_unix_nano

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return util.JSONLikeDictIter(
            ((x.key, _un_anyvalue(x.value)) for x in self._proto.attributes)
        )

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self._proto.dropped_attributes_count

    @property
    @override
    def otlp_events(self) -> Iterator[OTLPProtoSpanEvent]:
        return (OTLPProtoSpanEvent(event) for event in self._proto.events)

    @property
    @override
    def otlp_dropped_events_count(self) -> int:
        return self._proto.dropped_events_count

    @property
    @override
    def otlp_links(self) -> Iterator[OTLPProtoSpanLink]:
        return (OTLPProtoSpanLink(link) for link in self._proto.links)

    @property
    @override
    def otlp_dropped_links_count(self) -> int:
        return self._proto.dropped_links_count

    @property
    @override
    def otlp_status(self) -> OTLPProtoStatus:
        return OTLPProtoStatus(self._proto.status)


class OTLPProtoInstrumentationScope(base.InstrumentationScope):
    _proto: common.InstrumentationScope

    def __init__(self, proto: common.InstrumentationScope):
        self._proto = proto

    @property
    @override
    def otlp_name(self) -> str:
        return self._proto.name

    @property
    @override
    def otlp_version(self) -> Optional[str]:
        return self._proto.version if self._proto.version != "" else None

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return util.JSONLikeDictIter(
            ((x.key, _un_anyvalue(x.value)) for x in self._proto.attributes)
        )

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self._proto.dropped_attributes_count


class OTLPProtoResource(base.Resource):
    _proto: resource.Resource

    def __init__(self, proto: resource.Resource):
        self._proto = proto

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return util.JSONLikeDictIter(
            ((x.key, _un_anyvalue(x.value)) for x in self._proto.attributes)
        )

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self._proto.dropped_attributes_count


class OTLPProtoScopeSpanCollection(base.ScopeSpanCollection):
    _proto: trace.ScopeSpans

    def __init__(self, proto: trace.ScopeSpans):
        self._proto = proto

    @property
    @override
    def otlp_scope(self) -> OTLPProtoInstrumentationScope:
        return OTLPProtoInstrumentationScope(self._proto.scope)

    @property
    @override
    def otlp_spans(self) -> Iterator[OTLPProtoSpan]:
        return (OTLPProtoSpan(span) for span in self._proto.spans)

    @property
    @override
    def otlp_schema_url(self) -> Optional[str]:
        return self._proto.schema_url if self._proto.schema_url != "" else None


class OTLPProtoResourceSpanCollection(base.ResourceSpanCollection):
    _proto: trace.ResourceSpans

    def __init__(self, proto: trace.ResourceSpans):
        self._proto = proto

    @property
    @override
    def otlp_resource(self) -> OTLPProtoResource:
        return OTLPProtoResource(self._proto.resource)

    @property
    @override
    def otlp_scope_spans(self) -> Iterator[OTLPProtoScopeSpanCollection]:
        return (OTLPProtoScopeSpanCollection(span) for span in self._proto.scope_spans)

    @property
    @override
    def otlp_schema_url(self) -> Optional[str]:
        return self._proto.schema_url if self._proto.schema_url != "" else None


class OTLPProtoSpanCollection(base.SpanCollection):
    _proto: trace_collector.ExportTraceServiceRequest

    def __init__(self, proto: trace_collector.ExportTraceServiceRequest):
        self._proto = proto

    @property
    @override
    def otlp_resource_spans(self) -> Iterator[OTLPProtoResourceSpanCollection]:
        return (
            OTLPProtoResourceSpanCollection(span) for span in self._proto.resource_spans
        )


def load(fp: BinaryIO) -> OTLPProtoSpanCollection:
    return OTLPProtoSpanCollection(trace_collector.ExportTraceServiceRequest().load(fp))


def loads(s: bytes) -> OTLPProtoSpanCollection:
    return OTLPProtoSpanCollection(trace_collector.ExportTraceServiceRequest().parse(s))
