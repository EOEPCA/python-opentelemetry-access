from collections.abc import Iterator
from typing import Tuple, Optional
import json

from .. import base
from .. import util


def iter_otlp_jsonlike_anyvalue(jsonlike: util.JSONLike) -> util.JSONLikeIter:
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


def iter_otlp_jsonlike_kv(jsonlike: util.JSONLike) -> Tuple[str, util.JSONLikeIter]:
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


def iter_otlp_jsonlike_dict(jsonlike: util.JSONLikeList) -> util.JSONLikeDictIter:
    return util.JSONLikeDictIter((iter_otlp_jsonlike_kv(x) for x in jsonlike))


class OTLPJsonSpanEvent(base.SpanEvent):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_time_unix_nano(self) -> int:
        return int(util._expect_field_type(self.jobj, "timeUnixNano", str))

    @property
    def otlp_name(self) -> str:
        return util._expect_field_type(self.jobj, "name", str)

    @property
    def otlp_attributes(self) -> util.JSONLikeDictIter:
        return iter_otlp_jsonlike_dict(
            util._expect_field_type(
                self.jobj, "attributes", list, optional=True, default={}
            )
        )

    @property
    def otlp_dropped_attributes_count(self) -> int:
        return util._expect_field_type(
            self.jobj, "droppedAttributesCount", int, optional=True, default=0
        )


class OTLPJsonStatus(base.Status):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_message(self) -> Optional[str]:
        return util._expect_field_type(self.jobj, "message", str, optional=True)

    ## TODO: Make sure this is the correct defaulting behaviour
    @property
    def otlp_code(self) -> int:
        return util._expect_field_type(self.jobj, "code", int, optional=True, default=0)


class OTLPJsonSpanKind(base.SpanKind):
    def __init__(self, code: int):
        self.code = code

    @property
    def otlp_kind_code(self) -> int:
        return self.code


class OTLPJsonSpanLink(base.SpanLink):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_trace_id(self) -> str:
        return util._expect_field_type(self.jobj, "traceId", str)

    @property
    def otlp_span_id(self) -> str:
        return util._expect_field_type(self.jobj, "spanId", str)

    @property
    def otlp_state(self) -> str:
        return util._expect_field_type(self.jobj, "state", str)

    @property
    def otlp_attributes(self) -> util.JSONLikeDictIter:
        return iter_otlp_jsonlike_dict(
            util._expect_field_type(
                self.jobj, "attributes", list, optional=True, default={}
            )
        )

    @property
    def otlp_dropped_attributes_count(self) -> int:
        return util._expect_field_type(
            self.jobj, "droppedAttributesCount", int, optional=True, default=0
        )

    @property
    def otlp_flags(self) -> int:
        return util._expect_field_type(
            self.jobj, "flags", int, optional=True, default=0
        )


class OTLPJsonSpan(base.Span):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_trace_id(self) -> str:
        return util._expect_field_type(self.jobj, "traceId", str)

    @property
    def otlp_span_id(self) -> str:
        return util._expect_field_type(self.jobj, "spanId", str)

    @property
    def otlp_trace_state(self) -> Optional[str]:
        return util._expect_field_type(self.jobj, "traceState", str, optional=True)

    @property
    def otlp_parent_span_id(self) -> str:
        return util._expect_field_type(self.jobj, "parentSpanId", str)

    @property
    def otlp_flags(self) -> int:
        return util._expect_field_type(
            self.jobj, "flags", int, optional=True, default=0
        )

    @property
    def otlp_name(self) -> str:
        return util._expect_field_type(self.jobj, "name", str)

    @property
    def otlp_kind(self) -> OTLPJsonSpanKind:
        return OTLPJsonSpanKind(util._expect_field_type(self.jobj, "kind", int))

    @property
    def otlp_start_time_unix_nano(self) -> int:
        return int(util._expect_field_type(self.jobj, "startTimeUnixNano", str))

    @property
    def otlp_end_time_unix_nano(self) -> int:
        return int(util._expect_field_type(self.jobj, "endTimeUnixNano", str))

    @property
    def otlp_attributes(self) -> util.JSONLikeDictIter:
        return iter_otlp_jsonlike_dict(
            util._expect_field_type(
                self.jobj, "attributes", list, optional=True, default={}
            )
        )

    @property
    def otlp_dropped_attributes_count(self) -> int:
        return util._expect_field_type(
            self.jobj, "droppedAttributesCount", int, optional=True, default=0
        )

    @property
    def otlp_events(self) -> Iterator[OTLPJsonSpanEvent]:
        return util.ListLikeDumpIterator(
            (
                OTLPJsonSpanEvent(util.expect_dict(event))
                for event in util._expect_field_type(
                    self.jobj, "events", list, optional=True, default=[]
                )
            )
        )

    @property
    def otlp_dropped_events_count(self) -> int:
        return util._expect_field_type(
            self.jobj, "droppedEventsCount", int, optional=True, default=0
        )

    @property
    def otlp_links(self) -> Iterator[OTLPJsonSpanLink]:
        return util.ListLikeDumpIterator(
            (
                OTLPJsonSpanLink(util.expect_dict(link))
                for link in util._expect_field_type(
                    self.jobj, "links", list, optional=True, default=[]
                )
            )
        )

    @property
    def otlp_dropped_links_count(self) -> int:
        return util._expect_field_type(
            self.jobj, "droppedLinksCount", int, optional=True, default=0
        )

    @property
    def otlp_status(self) -> OTLPJsonStatus:
        return OTLPJsonStatus(util._expect_field_type(self.jobj, "status", dict))


class OTLPJsonInstrumentationScope(base.InstrumentationScope):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_name(self) -> str:
        return util._expect_field_type(self.jobj, "name", str)

    @property
    def otlp_version(self) -> Optional[str]:
        return util._expect_field_type(self.jobj, "version", str, optional=True)

    @property
    def otlp_attributes(self) -> util.JSONLikeDictIter:
        return iter_otlp_jsonlike_dict(
            util._expect_field_type(
                self.jobj, "attributes", list, optional=True, default={}
            )
        )

    @property
    def otlp_dropped_attributes_count(self) -> int:
        return util._expect_field_type(
            self.jobj, "droppedAttributesCount", int, optional=True, default=0
        )


class OTLPJsonResource(base.Resource):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_attributes(self) -> util.JSONLikeDictIter:
        return iter_otlp_jsonlike_dict(
            util._expect_field_type(
                self.jobj, "attributes", list, optional=True, default={}
            )
        )

    @property
    def otlp_dropped_attributes_count(self) -> int:
        return util._expect_field_type(
            self.jobj, "droppedAttributesCount", int, optional=True, default=0
        )


class OTLPJsonScopeSpanCollection(base.ScopeSpanCollection):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_scope(self) -> OTLPJsonInstrumentationScope:
        return OTLPJsonInstrumentationScope(
            util._expect_field_type(self.jobj, "scope", dict)
        )

    @property
    def otlp_spans(self) -> Iterator[OTLPJsonSpan]:
        return (
            OTLPJsonSpan(util.expect_dict(span))
            for span in util._expect_field_type(self.jobj, "spans", list)
        )

    @property
    def otlp_schema_url(self) -> str:
        return util._expect_field_type(self.jobj, "schemaUrl", str, optional=True)


class OTLPJsonResourceSpanCollection(base.ResourceSpanCollection):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_resource(self) -> OTLPJsonResource:
        return OTLPJsonResource(util._expect_field_type(self.jobj, "resource", dict))

    @property
    def otlp_scope_spans(self) -> Iterator[OTLPJsonScopeSpanCollection]:
        return (
            OTLPJsonScopeSpanCollection(util.expect_dict(span))
            for span in util._expect_field_type(self.jobj, "scopeSpans", list)
        )

    @property
    def otlp_schema_url(self) -> str:
        return util._expect_field_type(self.jobj, "schemaUrl", str, optional=True)


class OTLPJsonSpanCollection(base.SpanCollection):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_resource_spans(self) -> Iterator[OTLPJsonResourceSpanCollection]:
        return (
            OTLPJsonResourceSpanCollection(util.expect_dict(span))
            for span in util._expect_field_type(self.jobj, "resourceSpans", list)
        )
