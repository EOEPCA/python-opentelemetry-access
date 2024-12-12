from collections.abc import Iterator
from typing import Tuple, Optional, TextIO, BinaryIO, NewType, List, Union, override
from typing_extensions import TypedDict
import json

from .. import base
from .. import util


class OTLPJsonIntAnyValueRepresentation(TypedDict):
    intValue: int


class OTLPJsonStringAnyValueRepresentation(TypedDict):
    stringValue: str


class OTLPJsonDoubleAnyValueRepresentation(TypedDict):
    doubleValue: float


class OTLPJsonBoolAnyValueRepresentation(TypedDict):
    boolValue: bool


class OTLPJsonArrayAnyValueRepresentation(TypedDict):
    arrayValue: List["OTLPJsonAnyValueRepresentation"]


class OTLPJsonKVRepresentation(TypedDict):
    key: str
    value: "OTLPJsonAnyValueRepresentation"


class OTLPJsonKVListAnyValueRepresentation(TypedDict):
    kvlistValue: List[OTLPJsonKVRepresentation]


OTLPJsonAnyValueRepresentation = Union[
    OTLPJsonIntAnyValueRepresentation,
    OTLPJsonStringAnyValueRepresentation,
    OTLPJsonDoubleAnyValueRepresentation,
    OTLPJsonBoolAnyValueRepresentation,
    OTLPJsonArrayAnyValueRepresentation,
    OTLPJsonKVListAnyValueRepresentation,
]


def iter_otlp_jsonlike_anyvalue(
    jsonlike: OTLPJsonAnyValueRepresentation,
) -> util.JSONLikeIter:
    if not isinstance(jsonlike, dict) or not len(jsonlike) == 1:
        raise TypeError(
            f"OTLP JSON AnyValue expected to be a length 1 object, got {jsonlike}"
        )

    match jsonlike:
        case {"intValue": x}:
            if isinstance(x, str):
                return int(x)
            else:
                raise TypeError(
                    f"OTLP JSON intValue expected to be encoded as str, got {type(x)}"
                )
        case {"stringValue": x}:
            if isinstance(x, str):
                return x
            else:
                raise TypeError(
                    f"OTLP JSON stringValue expected to be encoded as str, got {type(x)}"
                )
        case {"doubleValue": x}:
            if isinstance(x, float):
                return x
            else:
                raise TypeError(
                    f"OTLP JSON doubleValue expected to be encoded as float, got {type(x)}"
                )
        case {"boolValue": x}:
            if isinstance(x, bool):
                return x
            else:
                raise TypeError(
                    f"OTLP JSON boolValue expected to be encoded as bool, got {type(x)}"
                )
        case {"arrayValue": xs}:
            if isinstance(xs, list):
                return util.JSONLikeListIter(
                    (iter_otlp_jsonlike_anyvalue(x) for x in xs)
                )
            else:
                raise TypeError(
                    f"OTLP JSON arrayValue expected to be encoded as list, got {type(xs)}"
                )
        case {"kvlistValue": xs}:
            if isinstance(xs, list):
                return iter_otlp_jsonlike_dict(xs)
            else:
                raise TypeError(
                    f"OTLP JSON kvlistValue expected to be encoded as list, got {type(xs)}"
                )
        case _:
            raise TypeError(f"Unexpected OTLP JSON AnyValue, {jsonlike}")


def iter_otlp_jsonlike_kv(
    jsonlike: OTLPJsonKVRepresentation,
) -> Tuple[str, util.JSONLikeIter]:
    match jsonlike:
        case {"key": key, "value": jsonlike_value, **rest}:
            if rest:
                raise TypeError(
                    f"OTLP JSON KeyValue expected to be of the form {{'key': ..., 'value': ...}}, got: '{json.dumps(jsonlike)}'"
                )

            if isinstance(key, str):
                return (key, iter_otlp_jsonlike_anyvalue(jsonlike_value))
            else:
                raise TypeError(f"OTLP JSON KeyValue key expected str, got {type(key)}")

        case _:
            raise TypeError(
                f"OTLP JSON KeyValue expected to be of the form {{'key': ..., 'value': ...}}, got: '{json.dumps(jsonlike)}'"
            )


def iter_otlp_jsonlike_dict(
    jsonlike: List[OTLPJsonKVRepresentation],
) -> util.JSONLikeDictIter:
    return util.JSONLikeDictIter((iter_otlp_jsonlike_kv(x) for x in jsonlike))


class OTLPJsonSpanEvent(base.SpanEvent):
    Representation = TypedDict(
        "Representation",
        {
            "timeUnixNano": str,
            "name": str,
            "attributes": List[OTLPJsonKVRepresentation],
            "droppedAttributesCount": Optional[int],
        },
    )

    def __init__(self, jobj: Representation):
        self.jobj = jobj

    @property
    @override
    def otlp_time_unix_nano(self) -> int:
        return int(self.jobj["timeUnixNano"])

    @property
    @override
    def otlp_name(self) -> str:
        return self.jobj["name"]

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return iter_otlp_jsonlike_dict(self.jobj.get("attributes") or [])

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self.jobj.get("droppedAttributesCount") or 0


class OTLPJsonStatus(base.Status):
    Representation = TypedDict(
        "Representation", {"message": Optional[str], "code": Optional[int]}
    )

    def __init__(self, jobj: Representation):
        self.jobj = jobj

    @property
    @override
    def otlp_message(self) -> Optional[str]:
        return self.jobj.get("message")

    ## TODO: Make sure this is the correct defaulting behaviour
    @property
    @override
    def otlp_code(self) -> int:
        return self.jobj.get("code") or 0


class OTLPJsonSpanKind(base.SpanKind):
    Representation = NewType("Representation", int)

    def __init__(self, code: Representation):
        self.code = code

    @property
    @override
    def otlp_kind_code(self) -> int:
        return self.code


class OTLPJsonSpanLink(base.SpanLink):
    Representation = TypedDict(
        "Representation",
        {
            "traceId": str,
            "spanId": str,
            "state": str,
            "attributes": List[OTLPJsonKVRepresentation],
            "droppedAttributesCount": Optional[int],
            "flags": Optional[int],
        },
    )

    def __init__(self, jobj: Representation):
        self.jobj = jobj

    @property
    @override
    def otlp_trace_id(self) -> str:
        return self.jobj["traceId"]

    @property
    @override
    def otlp_span_id(self) -> str:
        return self.jobj["spanId"]

    @property
    @override
    def otlp_state(self) -> str:
        return self.jobj["state"]

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return iter_otlp_jsonlike_dict(self.jobj.get("attributes") or [])

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self.jobj.get("droppedAttributesCount") or 0

    @property
    @override
    def otlp_flags(self) -> int:
        return self.jobj.get("flags") or 0


class OTLPJsonSpan(base.Span):
    Representation = TypedDict(
        "Representation",
        {
            "traceId": str,
            "spanId": str,
            "traceState": Optional[str],
            "parentSpanId": str,
            "flags": Optional[int],
            "name": str,
            "kind": OTLPJsonSpanKind.Representation,
            "startTimeUnixNano": str,
            "endTimeUnixNano": str,
            "attributes": List[OTLPJsonKVRepresentation],
            "droppedAttributesCount": Optional[int],
            "events": Optional[List[OTLPJsonSpanEvent.Representation]],
            "droppedEventsCount": Optional[int],
            "links": Optional[List[OTLPJsonSpanLink.Representation]],
            "droppedLinksCount": Optional[int],
            "status": OTLPJsonStatus.Representation,
        },
    )

    def __init__(self, jobj: Representation):
        self.jobj = jobj

    @property
    @override
    def otlp_trace_id(self) -> str:
        return self.jobj["traceId"]

    @property
    @override
    def otlp_span_id(self) -> str:
        return self.jobj["spanId"]

    @property
    @override
    def otlp_trace_state(self) -> Optional[str]:
        return self.jobj.get("traceState")

    @property
    @override
    def otlp_parent_span_id(self) -> str:
        return self.jobj["parentSpanId"]

    @property
    @override
    def otlp_flags(self) -> int:
        return self.jobj.get("flags") or 0

    @property
    @override
    def otlp_name(self) -> str:
        return self.jobj["name"]

    @property
    @override
    def otlp_kind(self) -> OTLPJsonSpanKind:
        return OTLPJsonSpanKind(self.jobj["kind"])

    @property
    @override
    def otlp_start_time_unix_nano(self) -> int:
        return int(self.jobj["startTimeUnixNano"])

    @property
    @override
    def otlp_end_time_unix_nano(self) -> int:
        return int(self.jobj["endTimeUnixNano"])

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return iter_otlp_jsonlike_dict(self.jobj.get("attributes") or [])

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self.jobj.get("droppedAttributesCount") or 0

    @property
    @override
    def otlp_events(self) -> Iterator[OTLPJsonSpanEvent]:
        return util.ListLikeDumpIterator(
            (OTLPJsonSpanEvent(event) for event in self.jobj.get("events") or [])
        )

    @property
    @override
    def otlp_dropped_events_count(self) -> int:
        return self.jobj.get("droppedEventsCount") or 0

    @property
    @override
    def otlp_links(self) -> Iterator[OTLPJsonSpanLink]:
        return util.ListLikeDumpIterator(
            (OTLPJsonSpanLink(link) for link in self.jobj.get("links") or [])
        )

    @property
    @override
    def otlp_dropped_links_count(self) -> int:
        return self.jobj.get("droppedLinksCount") or 0

    @property
    @override
    def otlp_status(self) -> OTLPJsonStatus:
        return OTLPJsonStatus(self.jobj["status"])


class OTLPJsonInstrumentationScope(base.InstrumentationScope):
    Representation = TypedDict(
        "Representation",
        {
            "name": str,
            "version": Optional[str],
            "attributes": List[OTLPJsonKVRepresentation],
            "droppedAttributesCount": Optional[int],
        },
    )

    def __init__(self, jobj: Representation):
        self.jobj = jobj

    @property
    @override
    def otlp_name(self) -> str:
        return self.jobj["name"]

    @property
    @override
    def otlp_version(self) -> Optional[str]:
        return self.jobj.get("version")

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return iter_otlp_jsonlike_dict(self.jobj.get("attributes") or [])

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self.jobj.get("droppedAttributesCount") or 0


class OTLPJsonResource(base.Resource):
    Representation = TypedDict(
        "Representation",
        {
            "attributes": List[OTLPJsonKVRepresentation],
            "droppedAttributesCount": Optional[int],
        },
    )

    def __init__(self, jobj: Representation):
        self.jobj = jobj

    @property
    @override
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return iter_otlp_jsonlike_dict(self.jobj.get("attributes") or [])

    @property
    @override
    def otlp_dropped_attributes_count(self) -> int:
        return self.jobj.get("droppedAttributesCount") or 0


class OTLPJsonScopeSpanCollection(base.ScopeSpanCollection):
    Representation = TypedDict(
        "Representation",
        {
            "scope": OTLPJsonInstrumentationScope.Representation,
            "spans": List[OTLPJsonSpan.Representation],
            "schemaUrl": Optional[str],
        },
    )

    def __init__(self, jobj: Representation):
        self.jobj = jobj

    @property
    @override
    def otlp_scope(self) -> OTLPJsonInstrumentationScope:
        return OTLPJsonInstrumentationScope(self.jobj["scope"])

    @property
    @override
    def otlp_spans(self) -> Iterator[OTLPJsonSpan]:
        return (OTLPJsonSpan(span) for span in self.jobj["spans"])

    @property
    @override
    def otlp_schema_url(self) -> Optional[str]:
        return self.jobj.get("schemaUrl")


class OTLPJsonResourceSpanCollection(base.ResourceSpanCollection):
    Representation = TypedDict(
        "Representation",
        {
            "resource": OTLPJsonResource.Representation,
            "scopeSpans": List[OTLPJsonScopeSpanCollection.Representation],
            "schemaUrl": Optional[str],
        },
    )

    def __init__(self, jobj: Representation):
        self.jobj = jobj

    @property
    @override
    def otlp_resource(self) -> OTLPJsonResource:
        return OTLPJsonResource(self.jobj["resource"])

    @property
    @override
    def otlp_scope_spans(self) -> Iterator[OTLPJsonScopeSpanCollection]:
        return (OTLPJsonScopeSpanCollection(span) for span in self.jobj["scopeSpans"])

    @property
    @override
    def otlp_schema_url(self) -> Optional[str]:
        return self.jobj.get("schemaUrl")


class OTLPJsonSpanCollection(base.SpanCollection):
    Representation = TypedDict(
        "Representation",
        {
            "resourceSpans": List[OTLPJsonResourceSpanCollection.Representation],
        },
    )

    def __init__(self, jobj: Representation):
        self.jobj = jobj

    @property
    @override
    def otlp_resource_spans(self) -> Iterator[OTLPJsonResourceSpanCollection]:
        return (
            OTLPJsonResourceSpanCollection(span) for span in self.jobj["resourceSpans"]
        )


def loado(o: util.JSONLike) -> OTLPJsonSpanCollection:
    return OTLPJsonSpanCollection(o)  # type: ignore


def load(fp: TextIO | BinaryIO) -> OTLPJsonSpanCollection:
    return loado(json.load(fp))


def loads(s: str) -> OTLPJsonSpanCollection:
    return loado(json.loads(s))
