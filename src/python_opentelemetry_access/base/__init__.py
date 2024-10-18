from collections.abc import Iterator
from typing import List, Dict, Union, Protocol, Tuple, TypeVar, Generic, Optional
from abc import abstractmethod
import itertools
import json

import binascii

import opentelemetry_betterproto.opentelemetry.proto.common.v1 as common
import opentelemetry_betterproto.opentelemetry.proto.trace.v1 as trace
import opentelemetry_betterproto.opentelemetry.proto.collector.trace.v1 as trace_collector
import opentelemetry_betterproto.opentelemetry.proto.resource.v1 as resource

T = TypeVar("T")


class DumpIterator(Generic[T], Iterator[T]):
    iterator: Iterator[T]

    def __init__(self, iterable):
        try:
            head = next(iterable)
            self.iterator = itertools.chain([head], iterable)
            self._has_head = True
        except StopIteration:
            self.iterator = iter([])
            self._has_head = False

    def __iter__(self):
        return iter(self.iterator)

    def __next__(self):
        return next(self.iterator)

    def initially_empty(self) -> bool:
        return not self._has_head

    def __bool__(self) -> bool:
        return self._has_head


class ListLikeDumpIterator(DumpIterator[T], list):
    def __len__(self):
        raise NotImplementedError("JSONLikeListIter has no length")

    def __getitem__(self, i):
        raise NotImplementedError("JSONLikeListIter cannot getitem")

    def __setitem__(self, i):
        raise NotImplementedError("JSONLikeListIter cannot setitem")


class JSONLikeDictIter(DumpIterator[Tuple[str, "JSONLikeIter"]]):
    pass


class JSONLikeListIter(ListLikeDumpIterator["JSONLikeIter"]):
    pass


JSONLikeLiteralIter = Union[str, bool, int, float]
JSONLikeIter = Union[JSONLikeLiteralIter, JSONLikeDictIter, JSONLikeListIter]

JSONLikeDict = Dict[str, "JSONLike"]
JSONLikeList = List["JSONLike"]
JSONLikeLiteral = Union[str, bool, int, float]
JSONLike = Union[JSONLikeLiteral, JSONLikeDict, JSONLikeList]


def jsonlike_iter_to_any_value(jsiter: JSONLikeIter) -> common.AnyValue:
    if isinstance(jsiter, JSONLikeDictIter):
        return common.AnyValue(
            kvlist_value=common.KeyValueList(jsonlike_dict_iter_to_kvlist(jsiter))
        )
    elif isinstance(jsiter, JSONLikeListIter):
        return common.AnyValue(
            array_value=common.ArrayValue(
                [jsonlike_iter_to_any_value(x) for x in iter(jsiter)]
            )
        )
    elif isinstance(jsiter, str):
        return common.AnyValue(string_value=jsiter)
    ## Bool must be before int
    elif isinstance(jsiter, bool):
        return common.AnyValue(bool_value=jsiter)
    elif isinstance(jsiter, int):
        return common.AnyValue(int_value=jsiter)
    elif isinstance(jsiter, float):
        return common.AnyValue(double_value=jsiter)


def jsonlike_dict_iter_to_kvlist(jsiter: JSONLikeDictIter) -> List[common.KeyValue]:
    match jsiter:
        case JSONLikeDictIter(iterator=inner):
            return [
                common.KeyValue(key=k, value=jsonlike_iter_to_any_value(v))
                for k, v in inner
            ]


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


class OTLPJSONEncoder(json.JSONEncoder):
    ## TODO: Is there a better way to do this (without forcing the dict)?
    def default(self, o):
        if isinstance(o, JSONLikeDictIter):
            return {k: v for k, v in iter(o)}
        else:
            return super().default(o)


# def to_otlp_json_iter(self) -> JSONLikeIter:
#     def inner():
#         raise NotImplementedError("Not implemented yet")
#         return
#     return JSONLikeDictIter(inner())
#
# def to_otlp_protobuf(self) -> OTLPProtobufType:
#     raise NotImplementedError("Not implemented yet")
class OTLPData(Protocol):
    @abstractmethod
    def to_otlp_json_iter(self) -> JSONLikeIter:
        pass

    def to_otlp_json_str_iter(self) -> Iterator[str]:
        return OTLPJSONEncoder().iterencode(self.to_otlp_json_iter())

    def to_otlp_json(self) -> str:
        return "".join(self.to_otlp_json_str_iter())

    @abstractmethod
    def to_otlp_protobuf(self) -> OTLPProtobufType:
        pass

    def to_otlp_protobuf_bytes(self) -> bytes:
        return bytes(self.to_otlp_protobuf())


def _to_otlp_any_value(jval: JSONLikeIter) -> JSONLikeDictIter:
    ## Bool must be before int
    if isinstance(jval, bool):
        return JSONLikeDictIter(iter([("boolValue", jval)]))
    elif isinstance(jval, int):
        return JSONLikeDictIter(iter([("intValue", str(jval))]))
    elif isinstance(jval, str):
        return JSONLikeDictIter(iter([("stringValue", jval)]))
    elif isinstance(jval, float):
        return JSONLikeDictIter(iter([("doubleValue", jval)]))
    elif isinstance(jval, JSONLikeDictIter):
        return JSONLikeDictIter(iter([("kvlistValue", _to_kv_list(jval))]))
    elif isinstance(jval, JSONLikeListIter):
        return JSONLikeDictIter(
            iter(
                [
                    (
                        "arrayValue",
                        JSONLikeListIter((_to_otlp_any_value(x) for x in jval)),
                    )
                ]
            )
        )
    else:
        raise TypeError(f"Unexpected anytype {type(jval)}")


def _to_kv_list(jsdict: JSONLikeDictIter) -> JSONLikeListIter:
    return JSONLikeListIter(
        (
            JSONLikeDictIter(iter([("key", k), ("value", _to_otlp_any_value(v))]))
            for k, v in iter(jsdict)
        )
    )


class SpanEvent(OTLPData, Protocol):
    def to_otlp_json_iter(self) -> JSONLikeDictIter:
        def inner():
            yield ("timeUnixNano", str(self.otlp_time_unix_nano))
            yield ("name", self.otlp_name)

            attributes = self.otlp_attributes
            if not attributes.initially_empty():
                yield ("attributes", _to_kv_list(attributes))

            dropped = self.otlp_dropped_attributes_count
            if dropped and dropped != 0:
                yield ("droppedAttributesCount", dropped)

            return

        return JSONLikeDictIter(inner())

    def to_otlp_protobuf(self) -> trace.SpanEvent:
        return trace.SpanEvent(
            self.otlp_time_unix_nano,
            self.otlp_name,
            jsonlike_dict_iter_to_kvlist(self.otlp_attributes),
            self.otlp_dropped_attributes_count,
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
    def otlp_attributes(self) -> JSONLikeDictIter:
        pass

    @property
    @abstractmethod
    def otlp_dropped_attributes_count(self) -> int:
        pass


class Status(OTLPData, Protocol):
    def to_otlp_json_iter(self) -> JSONLikeDictIter:
        def inner():
            message = self.otlp_message
            if message is not None:
                yield ("message", message)

            code = self.otlp_code
            if code != 0:
                yield ("code", code)

            return

        return JSONLikeDictIter(inner())

    def to_otlp_protobuf(self) -> trace.Status:
        return trace.Status(
            self.otlp_message or "", trace.StatusStatusCode(self.otlp_code)
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
    def to_otlp_json_iter(self) -> int:
        return self.otlp_kind_code

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
    def to_otlp_json_iter(self) -> JSONLikeIter:
        def inner():
            yield ("traceId", self.otlp_trace_id)
            yield ("spanId", self.otlp_span_id)
            yield ("traceState", self.otlp_trace_state)

            attributes = self.otlp_attributes
            if not attributes.initially_empty():
                yield ("attributes", _to_kv_list(attributes))

            dropped = self.otlp_dropped_attributes_count
            if dropped != 0:
                yield ("droppedAttributesCount", dropped)

            flags = self.otlp_flags
            if flags:
                yield ("flags", flags)

            return

        return JSONLikeDictIter(inner())

    def to_otlp_protobuf(self) -> trace.SpanLink:
        return trace.SpanLink(
            trace_id=binascii.a2b_hex(self.otlp_trace_id),
            span_id=binascii.a2b_hex(self.otlp_span_id),
            trace_state=self.otlp_state,
            attributes=jsonlike_dict_iter_to_kvlist(self.otlp_attributes),
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
    def otlp_attributes(self) -> JSONLikeDictIter:
        pass

    @property
    @abstractmethod
    def otlp_dropped_attributes_count(self) -> int:
        pass

    @property
    @abstractmethod
    def otlp_flags(self) -> int:
        pass


class Span(OTLPData, Protocol):
    def to_otlp_json_iter(self) -> JSONLikeDictIter:
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

            attributes = self.otlp_attributes
            if not attributes.initially_empty():
                yield ("attributes", _to_kv_list(attributes))

            dropped = self.otlp_dropped_attributes_count
            if dropped != 0:
                yield ("droppedAttributesCount", dropped)

            flags = self.otlp_flags
            if flags:
                yield ("flags", flags)

            events = self.otlp_events
            if not events.initially_empty():
                yield (
                    "events",
                    JSONLikeListIter((event.to_otlp_json_iter() for event in events)),
                )

            yield ("status", self.otlp_status.to_otlp_json_iter())

            return

        return JSONLikeDictIter(inner())

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
            attributes=jsonlike_dict_iter_to_kvlist(self.otlp_attributes),
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
    def otlp_attributes(self) -> JSONLikeDictIter:
        pass

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
    def to_otlp_json_iter(self) -> JSONLikeIter:
        def inner():
            yield ("name", self.otlp_name)

            version = self.otlp_version
            if version is not None:
                yield ("version", version)

            attributes = self.otlp_attributes
            if not attributes.initially_empty():
                yield ("attributes", _to_kv_list(attributes))

            dropped = self.otlp_dropped_attributes_count
            if dropped != 0:
                yield ("droppedAttributesCount", dropped)

            return

        return JSONLikeDictIter(inner())

    def to_otlp_protobuf(self) -> common.InstrumentationScope:
        return common.InstrumentationScope(
            name=self.otlp_name,
            version=self.otlp_version or "",
            attributes=jsonlike_dict_iter_to_kvlist(self.otlp_attributes),
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
    def otlp_attributes(self) -> JSONLikeDictIter:
        pass

    @property
    @abstractmethod
    def otlp_dropped_attributes_count(self) -> int:
        pass


class Resource(OTLPData, Protocol):
    def to_otlp_json_iter(self) -> JSONLikeDictIter:
        def inner():
            attributes = self.otlp_attributes
            if not attributes.initially_empty():
                yield ("attributes", _to_kv_list(attributes))

            dropped = self.otlp_dropped_attributes_count
            if dropped != 0:
                yield ("droppedAttributesCount", dropped)

            return

        return JSONLikeDictIter(inner())

    def to_otlp_protobuf(self) -> resource.Resource:
        return resource.Resource(
            attributes=jsonlike_dict_iter_to_kvlist(self.otlp_attributes),
            dropped_attributes_count=self.otlp_dropped_attributes_count,
        )

    @property
    @abstractmethod
    def otlp_attributes(self) -> JSONLikeDictIter:
        pass

    @property
    @abstractmethod
    def otlp_dropped_attributes_count(self) -> int:
        pass


## TODO: Stick to original OTLP names and introduce some additional
## "SpanCollection" abstraction


class ScopeSpanCollection(OTLPData, Protocol):
    def to_otlp_json_iter(self) -> JSONLikeDictIter:
        def inner():
            yield ("scope", self.otlp_scope.to_otlp_json_iter())
            yield (
                "spans",
                JSONLikeListIter((x.to_otlp_json_iter() for x in self.otlp_spans)),
            )
            schema_url = self.otlp_schema_url
            if schema_url:
                yield ("schemaUrl", schema_url)
            return

        return JSONLikeDictIter(inner())

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


class ResourceSpanCollection(OTLPData, Protocol):
    def to_otlp_json_iter(self) -> JSONLikeDictIter:
        def inner():
            yield ("resource", self.otlp_resource.to_otlp_json_iter())
            yield (
                "scopeSpans",
                JSONLikeListIter(
                    (x.to_otlp_json_iter() for x in self.otlp_scope_spans)
                ),
            )
            schema_url = self.otlp_schema_url
            if schema_url:
                yield ("schemaUrl", schema_url)

            return

        return JSONLikeDictIter(inner())

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


class SpanCollection(OTLPData, Protocol):
    def to_otlp_json_iter(self) -> JSONLikeDictIter:
        def inner():
            yield (
                "resourceSpans",
                JSONLikeListIter(
                    (x.to_otlp_json_iter() for x in self.otlp_resource_spans)
                ),
            )

            return

        return JSONLikeDictIter(inner())

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
