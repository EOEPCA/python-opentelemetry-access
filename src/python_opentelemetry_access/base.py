from collections.abc import Iterator
from typing import Union, Protocol, Optional, Tuple, List, override
from abc import abstractmethod
import json
from dataclasses import dataclass

import binascii

import opentelemetry_betterproto.opentelemetry.proto.common.v1 as common
import opentelemetry_betterproto.opentelemetry.proto.trace.v1 as trace
import opentelemetry_betterproto.opentelemetry.proto.collector.trace.v1 as trace_collector
import opentelemetry_betterproto.opentelemetry.proto.resource.v1 as resource

from . import util

OTLPProtobufType = Union[
    trace.SpanSpanKind,
    trace.Status,
    trace.SpanEvent,
    trace.Span,
    trace_collector.ExportTraceServiceRequest,
    resource.Resource,
    trace.ResourceSpans,
    common.InstrumentationScope,
    trace.ScopeSpans,
    trace.SpanLink,
]

ReifiedType = Union[
    "ReifiedSpanEvent",
    "ReifiedStatus",
    "ReifiedSpanKind",
    "ReifiedSpanLink",
    "ReifiedSpan",
    "ReifiedInstrumentationScope",
    "ReifiedResource",
    "ReifiedScopeSpanCollection",
    "ReifiedResourceSpanCollection",
    "ReifiedSpanCollection",
]


class OTLPJSONEncoder(json.JSONEncoder):
    ## TODO: Is there a better way to do this (without forcing the dict)?
    @override
    def default(self, o):
        if isinstance(o, util.JSONLikeDictIter):
            return {k: v for k, v in iter(o)}
        else:
            return super().default(o)


# def to_otlp_json_iter(self) -> util.JSONLikeIter:
#     def inner():
#         raise NotImplementedError("Not implemented yet")
#         return
#     return util.JSONLikeDictIter(inner())
#
# def to_otlp_protobuf(self) -> OTLPProtobufType:
#     raise NotImplementedError("Not implemented yet")
class OTLPData(Protocol):
    @abstractmethod
    def to_otlp_json_iter(self) -> util.JSONLikeIter:
        pass
    
    def to_otlp_json_str_iter(self) -> Iterator[str]:
        return OTLPJSONEncoder().iterencode(self.to_otlp_json_iter())

    def to_otlp_json(self) -> str:
        return "".join(self.to_otlp_json_str_iter())

    @abstractmethod
    def to_otlp_protobuf(self) -> OTLPProtobufType:
        pass

    @abstractmethod
    def to_reified(self) -> ReifiedType:
        pass

    def to_otlp_protobuf_bytes(self) -> bytes:
        return bytes(self.to_otlp_protobuf())


class SpanEvent(OTLPData, Protocol):
    @override
    def to_reified(self) -> "ReifiedSpanEvent":
        return ReifiedSpanEvent(
            time_unix_nano=self.otlp_time_unix_nano,
            name=self.otlp_name,
            attributes=util.force_jsonlike_dict_iter(self.otlp_attributes_iter),
            dropped_attributes_count=self.otlp_dropped_attributes_count,
        )

    @override
    def to_otlp_json_iter(self) -> util.JSONLikeDictIter:
        def inner():
            yield ("timeUnixNano", str(self.otlp_time_unix_nano))
            yield ("name", self.otlp_name)

            attributes = self.otlp_attributes_iter
            if not attributes.initially_empty():
                yield ("attributes", util.to_kv_list_iter(attributes))

            dropped = self.otlp_dropped_attributes_count
            if dropped and dropped != 0:
                yield ("droppedAttributesCount", dropped)

            return

        return util.JSONLikeDictIter(inner())

    @override
    def to_otlp_protobuf(self) -> trace.SpanEvent:
        return trace.SpanEvent(
            time_unix_nano=self.otlp_time_unix_nano,
            name=self.otlp_name,
            attributes=util.jsonlike_dict_iter_to_kvlist(self.otlp_attributes_iter),
            dropped_attributes_count=self.otlp_dropped_attributes_count,
        )

    @property
    @abstractmethod
    def otlp_time_unix_nano(self) -> int:
        pass

    @property
    @abstractmethod
    def otlp_name(self) -> str:
        pass

    @property
    @abstractmethod
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        pass

    @property
    def otlp_attributes(self) -> util.JSONLikeDict:
        return util.force_jsonlike_dict_iter(self.otlp_attributes_iter)

    @property
    @abstractmethod
    def otlp_dropped_attributes_count(self) -> int:
        pass


class Status(OTLPData, Protocol):
    @override
    def to_reified(self) -> "ReifiedStatus":
        return ReifiedStatus(message=self.otlp_message, code=self.otlp_code)

    @override
    def to_otlp_json_iter(self) -> util.JSONLikeDictIter:
        def inner():
            message = self.otlp_message
            if message is not None:
                yield ("message", message)

            code = self.otlp_code
            if code != 0:
                yield ("code", code)

            return

        return util.JSONLikeDictIter(inner())

    @override
    def to_otlp_protobuf(self) -> trace.Status:
        return trace.Status(
            message=self.otlp_message or "", code=trace.StatusStatusCode(self.otlp_code)
        )

    @property
    @abstractmethod
    def otlp_message(self) -> Optional[str]:
        pass

    @property
    @abstractmethod
    def otlp_code(self) -> int:
        pass

    @property
    def otlp_status_name(self) -> str:
        return str(trace.StatusStatusCode(self.otlp_code))


class SpanKind(OTLPData, Protocol):
    @override
    def to_reified(self) -> "ReifiedSpanKind":
        return ReifiedSpanKind(kind_code=self.otlp_kind_code)

    @override
    def to_otlp_json_iter(self) -> int:
        return self.otlp_kind_code

    @override
    def to_otlp_protobuf(self) -> trace.SpanSpanKind:
        return trace.SpanSpanKind(self.otlp_kind_code)

    @property
    @abstractmethod
    def otlp_kind_code(self):
        pass

    @property
    def otlp_kind_name(self):
        return str(trace.SpanSpanKind(self.otlp_kind_code))


class SpanLink(OTLPData, Protocol):
    @override
    def to_reified(self) -> "ReifiedSpanLink":
        return ReifiedSpanLink(
            trace_id=self.otlp_trace_id,
            span_id=self.otlp_span_id,
            state=self.otlp_state,
            attributes=util.force_jsonlike_dict_iter(self.otlp_attributes_iter),
            dropped_attributes_count=self.otlp_dropped_attributes_count,
            flags=self.otlp_flags,
        )

    @override
    def to_otlp_json_iter(self) -> util.JSONLikeIter:
        def inner():
            yield ("traceId", self.otlp_trace_id)
            yield ("spanId", self.otlp_span_id)
            yield ("traceState", self.otlp_trace_state)

            attributes = self.otlp_attributes_iter
            if not attributes.initially_empty():
                yield ("attributes", util.to_kv_list_iter(attributes))

            dropped = self.otlp_dropped_attributes_count
            if dropped != 0:
                yield ("droppedAttributesCount", dropped)

            flags = self.otlp_flags
            if flags:
                yield ("flags", flags)

            return

        return util.JSONLikeDictIter(inner())

    @override
    def to_otlp_protobuf(self) -> trace.SpanLink:
        return trace.SpanLink(
            trace_id=binascii.a2b_hex(self.otlp_trace_id),
            span_id=binascii.a2b_hex(self.otlp_span_id),
            trace_state=self.otlp_state,
            attributes=util.jsonlike_dict_iter_to_kvlist(self.otlp_attributes_iter),
            dropped_attributes_count=self.otlp_dropped_attributes_count,
            flags=self.otlp_flags,
        )

    @property
    @abstractmethod
    def otlp_trace_id(self) -> str:
        pass

    @property
    @abstractmethod
    def otlp_span_id(self) -> str:
        pass

    @property
    @abstractmethod
    def otlp_state(self) -> str:
        pass

    @property
    @abstractmethod
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        pass

    @property
    def otlp_attributes(self) -> util.JSONLikeDict:
        return util.force_jsonlike_dict_iter(self.otlp_attributes_iter)

    @property
    @abstractmethod
    def otlp_dropped_attributes_count(self) -> int:
        pass

    @property
    @abstractmethod
    def otlp_flags(self) -> int:
        pass


class Span(OTLPData, Protocol):
    @override
    def to_reified(self) -> "ReifiedSpan":
        return ReifiedSpan(
            trace_id=self.otlp_trace_id,
            span_id=self.otlp_span_id,
            trace_state=self.otlp_trace_state,
            parent_span_id=self.otlp_parent_span_id,
            flags=self.otlp_flags,
            name=self.otlp_name,
            kind=ReifiedSpanKind(self.otlp_kind.to_otlp_protobuf()),
            start_time_unix_nano=self.otlp_start_time_unix_nano,
            end_time_unix_nano=self.otlp_end_time_unix_nano,
            attributes=util.force_jsonlike_dict_iter(self.otlp_attributes_iter),
            dropped_attributes_count=self.otlp_dropped_attributes_count,
            events=[x.to_reified() for x in self.otlp_events],
            dropped_events_count=self.otlp_dropped_events_count,
            links=[x.to_reified() for x in self.otlp_links],
            dropped_links_count=self.otlp_dropped_links_count,
            status=self.otlp_status.to_reified(),
        )

    @override
    def to_otlp_json_iter(self) -> util.JSONLikeDictIter:
        def inner():
            yield ("traceId", self.otlp_trace_id)
            yield ("spanId", self.otlp_span_id)

            traceState = self.otlp_trace_state
            if traceState:
                yield ("traceState", traceState)

            yield ("parentSpanId", self.otlp_parent_span_id)

            flags = self.otlp_flags
            if flags != 0:
                yield ("flags", flags)

            yield ("name", self.otlp_name)
            yield ("kind", self.otlp_kind.to_otlp_json_iter())
            yield ("startTimeUnixNano", str(self.otlp_start_time_unix_nano))
            yield ("endTimeUnixNano", str(self.otlp_end_time_unix_nano))

            attributes = self.otlp_attributes_iter
            if not attributes.initially_empty():
                yield ("attributes", util.to_kv_list_iter(attributes))

            dropped = self.otlp_dropped_attributes_count
            if dropped != 0:
                yield ("droppedAttributesCount", dropped)

            flags = self.otlp_flags
            if flags:
                yield ("flags", flags)

            peek_events = util.peek_iterator(self.otlp_events)
            if peek_events:
                _, events = peek_events
                yield (
                    "events",
                    util.JSONLikeListIter(
                        (event.to_otlp_json_iter() for event in events)
                    ),
                )

            yield ("status", self.otlp_status.to_otlp_json_iter())

            return

        return util.JSONLikeDictIter(inner())

    @override
    def to_otlp_protobuf(self) -> trace.Span:
        return trace.Span(
            trace_id=binascii.a2b_hex(self.otlp_trace_id),
            span_id=binascii.a2b_hex(self.otlp_span_id),
            trace_state=self.otlp_trace_state or "",
            parent_span_id=binascii.a2b_hex(self.otlp_parent_span_id),
            flags=self.otlp_flags,
            name=self.otlp_name,
            kind=self.otlp_kind.to_otlp_protobuf(),
            start_time_unix_nano=self.otlp_start_time_unix_nano,
            end_time_unix_nano=self.otlp_end_time_unix_nano,
            attributes=util.jsonlike_dict_iter_to_kvlist(self.otlp_attributes_iter),
            dropped_attributes_count=self.otlp_dropped_attributes_count,
            events=[x.to_otlp_protobuf() for x in self.otlp_events],
            dropped_events_count=self.otlp_dropped_events_count,
            links=[x.to_otlp_protobuf() for x in self.otlp_links],
            dropped_links_count=self.otlp_dropped_links_count,
            status=self.otlp_status.to_otlp_protobuf(),
        )

    @property
    @abstractmethod
    def otlp_trace_id(self) -> str:
        pass

    @property
    @abstractmethod
    def otlp_span_id(self) -> str:
        pass

    @property
    @abstractmethod
    def otlp_trace_state(self) -> Optional[str]:
        pass

    @property
    @abstractmethod
    def otlp_parent_span_id(self) -> str:
        pass

    @property
    @abstractmethod
    def otlp_flags(self) -> int:
        pass

    @property
    @abstractmethod
    def otlp_name(self) -> str:
        pass

    @property
    @abstractmethod
    def otlp_kind(self) -> SpanKind:
        pass

    @property
    @abstractmethod
    def otlp_start_time_unix_nano(self) -> int:
        pass

    @property
    @abstractmethod
    def otlp_end_time_unix_nano(self) -> int:
        pass

    @property
    @abstractmethod
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        pass

    @property
    def otlp_attributes(self) -> util.JSONLikeDict:
        return util.force_jsonlike_dict_iter(self.otlp_attributes_iter)

    @property
    @abstractmethod
    def otlp_dropped_attributes_count(self) -> int:
        pass

    @property
    @abstractmethod
    def otlp_events(self) -> Iterator[SpanEvent]:
        pass

    @property
    @abstractmethod
    def otlp_dropped_events_count(self) -> int:
        pass

    @property
    @abstractmethod
    def otlp_links(self) -> Iterator[SpanLink]:
        pass

    @property
    @abstractmethod
    def otlp_dropped_links_count(self) -> int:
        pass

    @property
    @abstractmethod
    def otlp_status(self) -> Status:
        pass


class InstrumentationScope(OTLPData, Protocol):
    @override
    def to_reified(self) -> "ReifiedInstrumentationScope":
        return ReifiedInstrumentationScope(
            name=self.otlp_name,
            version=self.otlp_version,
            attributes=util.force_jsonlike_dict_iter(self.otlp_attributes_iter),
            dropped_attributes_count=self.otlp_dropped_attributes_count,
        )

    @override
    def to_otlp_json_iter(self) -> util.JSONLikeIter:
        def inner():
            yield ("name", self.otlp_name)

            version = self.otlp_version
            if version is not None:
                yield ("version", version)

            attributes = self.otlp_attributes_iter
            if not attributes.initially_empty():
                yield ("attributes", util.to_kv_list_iter(attributes))

            dropped = self.otlp_dropped_attributes_count
            if dropped != 0:
                yield ("droppedAttributesCount", dropped)

            return

        return util.JSONLikeDictIter(inner())

    @override
    def to_otlp_protobuf(self) -> common.InstrumentationScope:
        return common.InstrumentationScope(
            name=self.otlp_name,
            version=self.otlp_version or "",
            attributes=util.jsonlike_dict_iter_to_kvlist(self.otlp_attributes_iter),
            dropped_attributes_count=self.otlp_dropped_attributes_count,
        )

    @property
    @abstractmethod
    def otlp_name(self) -> str:
        pass

    @property
    @abstractmethod
    def otlp_version(self) -> Optional[str]:
        pass

    @property
    @abstractmethod
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        pass

    @property
    def otlp_attributes(self) -> util.JSONLikeDict:
        return util.force_jsonlike_dict_iter(self.otlp_attributes_iter)

    @property
    @abstractmethod
    def otlp_dropped_attributes_count(self) -> int:
        pass


class Resource(OTLPData, Protocol):
    @override
    def to_reified(self) -> "ReifiedResource":
        return ReifiedResource(
            attributes=util.force_jsonlike_dict_iter(self.otlp_attributes_iter),
            dropped_attributes_count=self.otlp_dropped_attributes_count,
        )

    @override
    def to_otlp_json_iter(self) -> util.JSONLikeDictIter:
        def inner():
            attributes = self.otlp_attributes_iter
            if not attributes.initially_empty():
                yield ("attributes", util.to_kv_list_iter(attributes))

            dropped = self.otlp_dropped_attributes_count
            if dropped != 0:
                yield ("droppedAttributesCount", dropped)

            return

        return util.JSONLikeDictIter(inner())

    @override
    def to_otlp_protobuf(self) -> resource.Resource:
        return resource.Resource(
            attributes=util.jsonlike_dict_iter_to_kvlist(self.otlp_attributes_iter),
            dropped_attributes_count=self.otlp_dropped_attributes_count,
        )

    @property
    @abstractmethod
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        pass

    @property
    def otlp_attributes(self) -> util.JSONLikeDict:
        return util.force_jsonlike_dict_iter(self.otlp_attributes_iter)

    @property
    @abstractmethod
    def otlp_dropped_attributes_count(self) -> int:
        pass


## TODO: Stick to original OTLP names and introduce some additional
## "SpanCollection" abstraction


class ScopeSpanCollection(OTLPData, Protocol):
    @override
    def to_reified(self) -> "ReifiedScopeSpanCollection":
        return ReifiedScopeSpanCollection(
            scope=self.otlp_scope.to_reified(),
            spans=[span.to_reified() for span in self.otlp_spans],
            schema_url=self.otlp_schema_url,
        )

    @override
    def to_otlp_json_iter(self) -> util.JSONLikeDictIter:
        def inner():
            yield ("scope", self.otlp_scope.to_otlp_json_iter())
            yield (
                "spans",
                util.JSONLikeListIter((x.to_otlp_json_iter() for x in self.otlp_spans)),
            )
            schema_url = self.otlp_schema_url
            if schema_url:
                yield ("schemaUrl", schema_url)
            return

        return util.JSONLikeDictIter(inner())

    @override
    def to_otlp_protobuf(self) -> trace.ScopeSpans:
        return trace.ScopeSpans(
            scope=self.otlp_scope.to_otlp_protobuf(),
            spans=[span.to_otlp_protobuf() for span in self.otlp_spans],
            schema_url=self.otlp_schema_url or "",
        )

    @property
    @abstractmethod
    def otlp_scope(self) -> InstrumentationScope:
        pass

    @property
    @abstractmethod
    def otlp_spans(self) -> Iterator[Span]:
        pass

    @property
    @abstractmethod
    def otlp_schema_url(self) -> Optional[str]:
        pass

    def iter_spans(self) -> Iterator[Span]:
        return self.otlp_spans


class ResourceSpanCollection(OTLPData, Protocol):
    @override
    def to_reified(self) -> "ReifiedResourceSpanCollection":
        return ReifiedResourceSpanCollection(
            resource=self.otlp_resource.to_reified(),
            scope_spans=[
                scope_spans.to_reified() for scope_spans in self.otlp_scope_spans
            ],
            schema_url=self.otlp_schema_url,
        )

    @override
    def to_otlp_json_iter(self) -> util.JSONLikeDictIter:
        def inner():
            yield ("resource", self.otlp_resource.to_otlp_json_iter())
            yield (
                "scopeSpans",
                util.JSONLikeListIter(
                    (x.to_otlp_json_iter() for x in self.otlp_scope_spans)
                ),
            )
            schema_url = self.otlp_schema_url
            if schema_url:
                yield ("schemaUrl", schema_url)

            return

        return util.JSONLikeDictIter(inner())

    @override
    def to_otlp_protobuf(self) -> trace.ResourceSpans:
        return trace.ResourceSpans(
            resource=self.otlp_resource.to_otlp_protobuf(),
            scope_spans=[
                scope_spans.to_otlp_protobuf() for scope_spans in self.otlp_scope_spans
            ],
            schema_url=self.otlp_schema_url or "",
        )

    @property
    @abstractmethod
    def otlp_resource(self) -> Resource:
        pass

    @property
    @abstractmethod
    def otlp_scope_spans(self) -> Iterator[ScopeSpanCollection]:
        pass

    @property
    @abstractmethod
    def otlp_schema_url(self) -> Optional[str]:
        pass

    def iter_spans(self) -> Iterator[Tuple[InstrumentationScope, Span]]:
        for ssc in self.otlp_scope_spans:
            scope = ssc.otlp_scope
            for span in ssc.iter_spans():
                yield (scope, span)


class SpanCollection(OTLPData, Protocol):
    @override
    def to_reified(self) -> "ReifiedSpanCollection":
        return ReifiedSpanCollection(
            resource_spans=[
                resource_spans.to_reified()
                for resource_spans in self.otlp_resource_spans
            ]
        )

    @override
    def to_otlp_json_iter(self) -> util.JSONLikeDictIter:
        def inner():
            yield (
                "resourceSpans",
                util.JSONLikeListIter(
                    (x.to_otlp_json_iter() for x in self.otlp_resource_spans)
                ),
            )

            return

        return util.JSONLikeDictIter(inner())

    @override
    def to_otlp_protobuf(self) -> trace_collector.ExportTraceServiceRequest:
        return trace_collector.ExportTraceServiceRequest(
            resource_spans=[
                resource_spans.to_otlp_protobuf()
                for resource_spans in self.otlp_resource_spans
            ]
        )

    @property
    @abstractmethod
    def otlp_resource_spans(self) -> Iterator[ResourceSpanCollection]:
        pass

    def iter_spans(self) -> Iterator[Tuple[Resource, InstrumentationScope, Span]]:
        for rsc in self.otlp_resource_spans:
            resource = rsc.otlp_resource
            for scope, span in rsc.iter_spans():
                yield (resource, scope, span)


def iter_jsonlike(jobj: util.JSONLike) -> util.JSONLikeIter:
    if isinstance(jobj, dict):
        return iter_jsonlike_dict(jobj)
    elif isinstance(jobj, list):
        return iter_jsonlike_list(jobj)
    elif isinstance(jobj, int) | isinstance(jobj, str) | isinstance(jobj, float):
        return jobj
    else:
        raise TypeError(f"Expecte JSONLike, got {type(jobj)}")


def iter_jsonlike_list(jobj: util.JSONLikeList) -> util.JSONLikeListIter:
    return util.JSONLikeListIter((iter_jsonlike(x) for x in jobj))


def iter_jsonlike_dict(jobj: util.JSONLikeDict) -> util.JSONLikeDictIter:
    return util.JSONLikeDictIter(((k, iter_jsonlike(v)) for k, v in jobj.items()))


@dataclass
class ReifiedSpanEvent(SpanEvent):
    time_unix_nano: int

    @property
    @override
    def otlp_time_unix_nano(self) -> int:
        return self.time_unix_nano

    name: str

    @property
    @override
    def otlp_name(self) -> str:
        return self.name

    attributes: util.JSONLikeDict

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return iter_jsonlike_dict(self.attributes)

    dropped_attributes_count: int

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self.dropped_attributes_count


@dataclass
class ReifiedStatus(Status):
    message: Optional[str]

    @property
    @override
    def otlp_message(self) -> Optional[str]:
        return self.message

    code: int

    @property
    @override
    def otlp_code(self) -> int:
        return self.code


@dataclass
class ReifiedSpanKind(SpanKind):
    kind_code: int

    @property
    @override
    def otlp_kind_code(self):
        return self.kind_code


@dataclass
class ReifiedSpanLink(SpanLink):
    trace_id: str

    @property
    @override
    def otlp_trace_id(self) -> str:
        return self.trace_id

    span_id: str

    @property
    @override
    def otlp_span_id(self) -> str:
        return self.span_id

    state: str

    @property
    @override
    def otlp_state(self) -> str:
        return self.state

    attributes: util.JSONLikeDict

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return iter_jsonlike_dict(self.attributes)

    dropped_attributes_count: int

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self.dropped_attributes_count

    flags: int

    @property
    @override
    def otlp_flags(self) -> int:
        return self.flags


@dataclass
class ReifiedSpan(Span):
    trace_id: str

    @property
    @override
    def otlp_trace_id(self) -> str:
        return self.trace_id

    span_id: str

    @property
    @override
    def otlp_span_id(self) -> str:
        return self.span_id

    trace_state: Optional[str]

    @property
    @override
    def otlp_trace_state(self) -> Optional[str]:
        return self.trace_state

    parent_span_id: str

    @property
    @override
    def otlp_parent_span_id(self) -> str:
        return self.parent_span_id

    flags: int

    @property
    @override
    def otlp_flags(self) -> int:
        return self.flags

    name: str

    @property
    @override
    def otlp_name(self) -> str:
        return self.name

    kind: ReifiedSpanKind

    @property
    @override
    def otlp_kind(self) -> ReifiedSpanKind:
        return self.kind

    start_time_unix_nano: int

    @property
    @override
    def otlp_start_time_unix_nano(self) -> int:
        return self.start_time_unix_nano

    end_time_unix_nano: int

    @property
    @override
    def otlp_end_time_unix_nano(self) -> int:
        return self.end_time_unix_nano

    attributes: util.JSONLikeDict

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return iter_jsonlike_dict(self.attributes)

    dropped_attributes_count: int

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self.dropped_attributes_count

    events: List[ReifiedSpanEvent]

    @property
    @override
    def otlp_events(self) -> Iterator[ReifiedSpanEvent]:
        return iter(self.events)

    dropped_events_count: int

    @property
    @override
    def otlp_dropped_events_count(self) -> int:
        return self.dropped_events_count

    links: List[ReifiedSpanLink]

    @property
    @override
    def otlp_links(self) -> Iterator[ReifiedSpanLink]:
        return iter(self.links)

    dropped_links_count: int

    @property
    @override
    def otlp_dropped_links_count(self) -> int:
        return self.dropped_links_count

    status: ReifiedStatus

    @property
    @override
    def otlp_status(self) -> ReifiedStatus:
        return self.status


@dataclass
class ReifiedInstrumentationScope(InstrumentationScope):
    name: str

    @property
    @override
    def otlp_name(self) -> str:
        return self.name

    version: Optional[str]

    @property
    @override
    def otlp_version(self) -> Optional[str]:
        return self.version

    attributes: util.JSONLikeDict

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return iter_jsonlike_dict(self.attributes)

    dropped_attributes_count: int

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self.dropped_attributes_count


@dataclass
class ReifiedResource(Resource):
    attributes: util.JSONLikeDict

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return iter_jsonlike_dict(self.attributes)

    dropped_attributes_count: int

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self.dropped_attributes_count


## TODO: Stick to original Reified names and introduce some additional
## "SpanCollection" abstraction


@dataclass
class ReifiedScopeSpanCollection(ScopeSpanCollection):
    scope: ReifiedInstrumentationScope

    @property
    @override
    def otlp_scope(self) -> ReifiedInstrumentationScope:
        return self.scope

    spans: List[ReifiedSpan]

    @property
    @override
    def otlp_spans(self) -> Iterator[ReifiedSpan]:
        return iter(self.spans)

    schema_url: Optional[str]

    @property
    @override
    def otlp_schema_url(self) -> Optional[str]:
        return self.schema_url


@dataclass
class ReifiedResourceSpanCollection(ResourceSpanCollection):
    resource: ReifiedResource

    @property
    @override
    def otlp_resource(self) -> ReifiedResource:
        return self.resource

    scope_spans: List[ReifiedScopeSpanCollection]

    @property
    @override
    def otlp_scope_spans(self) -> Iterator[ReifiedScopeSpanCollection]:
        return iter(self.scope_spans)

    schema_url: Optional[str]

    @property
    @override
    def otlp_schema_url(self) -> Optional[str]:
        return self.schema_url


@dataclass
class ReifiedSpanCollection(SpanCollection):
    resource_spans: List[ReifiedResourceSpanCollection]

    @property
    @override
    def otlp_resource_spans(self) -> Iterator[ReifiedResourceSpanCollection]:
        return iter(self.resource_spans)
