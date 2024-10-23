from collections.abc import Iterator
from typing import Optional, Dict, List
# import json

from itertools import groupby

from .. import base
from .. import util

## Only needed for correctly parsing nanosecond timestamps
from pandas import Timestamp
import opentelemetry_betterproto.opentelemetry.proto.trace.v1 as trace


def _parse_ns_isotime(t: str) -> int:
    return int(Timestamp(t).asm8.astype("datetime64[ns]"))


def _iter_simple_jsonlike(jobj: util.JSONLike) -> util.JSONLikeIter:
    if (
        isinstance(jobj, int)
        or isinstance(jobj, float)
        or isinstance(jobj, str)
        or isinstance(jobj, bool)
    ):
        return jobj
    elif isinstance(jobj, dict):
        return _iter_simple_jsonlike_dict(jobj)
    elif isinstance(jobj, list):
        return _iter_simple_jsonlike_list(jobj)


def _iter_simple_jsonlike_list(jobj: util.JSONLikeList) -> util.JSONLikeListIter:
    return util.JSONLikeListIter((_iter_simple_jsonlike(x) for x in jobj))


def _iter_simple_jsonlike_dict(jobj: util.JSONLikeDict) -> util.JSONLikeDictIter:
    return util.JSONLikeDictIter(
        ((k, _iter_simple_jsonlike(v)) for k, v in jobj.items())
    )


class SS4OSpanEvent(base.SpanEvent):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_time_unix_nano(self) -> int:
        return _parse_ns_isotime(util._expect_field_type(self.jobj, "@timestamp", str))

    @property
    def otlp_name(self) -> str:
        return util._expect_field_type(self.jobj, "name", str)

    @property
    def otlp_attributes(self) -> util.JSONLikeDictIter:
        return _iter_simple_jsonlike_dict(
            util._expect_field_type(
                self.jobj, "attributes", dict, optional=True, default={}
            )
        )

    @property
    def otlp_dropped_attributes_count(self) -> int:
        return util._expect_field_type(
            self.jobj, "droppedAttributesCount", int, optional=True, default=0
        )


class SS4OStatus(base.Status):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_message(self) -> Optional[str]:
        message = util._expect_field_type(self.jobj, "message", str, optional=True)
        return message if message != "" else None

    ## TODO: Make sure this is the correct defaulting behaviour
    @property
    def otlp_code(self) -> int:
        return trace.StatusStatusCode.from_string(
            "STATUS_CODE_"
            + util._expect_field_type(
                self.jobj, "code", str, optional=True, default=0
            ).upper()
        )


class SS4OSpanKind(base.SpanKind):
    def __init__(self, name: str):
        self.name = name

    @property
    def otlp_kind_code(self) -> int:
        return trace.SpanSpanKind.from_string("SPAN_KIND_" + self.name.upper())


class SS4OSpanLink(base.SpanLink):
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
        return _iter_simple_jsonlike_dict(
            util._expect_field_type(
                self.jobj, "attributes", dict, optional=True, default={}
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


class SS4OSpan(base.Span):
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
    def otlp_kind(self) -> SS4OSpanKind:
        return SS4OSpanKind(util._expect_field_type(self.jobj, "kind", str))

    @property
    def otlp_start_time_unix_nano(self) -> int:
        return _parse_ns_isotime(util._expect_field_type(self.jobj, "startTime", str))

    @property
    def otlp_end_time_unix_nano(self) -> int:
        return _parse_ns_isotime(util._expect_field_type(self.jobj, "endTime", str))

    @property
    def otlp_attributes(self) -> util.JSONLikeDictIter:
        return _iter_simple_jsonlike_dict(
            util._expect_field_type(
                self.jobj, "attributes", dict, optional=True, default={}
            )
        )

    @property
    def otlp_dropped_attributes_count(self) -> int:
        return util._expect_field_type(
            self.jobj, "droppedAttributesCount", int, optional=True, default=0
        )

    @property
    def otlp_events(self) -> Iterator[SS4OSpanEvent]:
        return util.ListLikeDumpIterator(
            (
                SS4OSpanEvent(util.expect_dict(event))
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
    def otlp_links(self) -> Iterator[SS4OSpanLink]:
        return util.ListLikeDumpIterator(
            (
                SS4OSpanLink(util.expect_dict(link))
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
    def otlp_status(self) -> SS4OStatus:
        return SS4OStatus(util._expect_field_type(self.jobj, "status", dict))


class SS4OInstrumentationScope(base.InstrumentationScope):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_name(self) -> str:
        return util._expect_field_type(self.jobj, "name", str)

    @property
    def otlp_version(self) -> Optional[str]:
        version = util._expect_field_type(self.jobj, "version", str, optional=True)
        return version if version != "" else None

    @property
    def otlp_attributes(self) -> util.JSONLikeDictIter:
        return _iter_simple_jsonlike_dict(
            util._expect_field_type(
                self.jobj, "attributes", dict, optional=True, default={}
            )
        )

    @property
    def otlp_dropped_attributes_count(self) -> int:
        ## TODO: Need to figure out how this is handled
        return 0


def _from_flattened(jobj: Dict[str, util.JSONLikeLiteral]) -> util.JSONLikeDictIter:
    def outer(kvs_outer):
        for k_primary, kvs_inner in groupby(
            map(lambda kv: (kv[0].split(".", 1), kv[1]), kvs_outer),
            key=lambda kv: kv[0][0],
        ):
            yield (k_primary, inner(kvs_inner))

    def inner(kvs):
        kv, kvs = util.peek_iterator(kvs)
        if len(kv[0]) == 1:
            return kv[1]
        else:
            return util.JSONLikeDictIter(outer(((k[1], v) for k, v in kvs)))

    return util.JSONLikeDictIter(outer(jobj.items()))


# def _fff(jobj: Dict[str, util.JSONLikeLiteral]) -> util.JSONLikeDict:
#     return util.force_jsonlike_dict_iter(_from_flattened(jobj))


class SS4OResource(base.Resource):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_attributes(self) -> util.JSONLikeDictIter:
        return _from_flattened(
            util._expect_field_type(
                self.jobj, "attributes", dict, optional=True, default={}
            )
        )

    @property
    def otlp_dropped_attributes_count(self) -> int:
        ## TODO: Need to determine how this is handed
        return 0


class SS4OScopeSpanCollection(base.ScopeSpanCollection):
    def __init__(
        self,
        scope: SS4OInstrumentationScope,
        search_results: Iterator[util.JSONLikeDict],
    ):
        self.scope = scope
        self.search_results = search_results

    @property
    def otlp_scope(self) -> SS4OInstrumentationScope:
        return self.scope

    @property
    def otlp_spans(self) -> Iterator[SS4OSpan]:
        return (SS4OSpan(span) for span in self.search_results)

    @property
    def otlp_schema_url(self) -> str:
        ## TODO: Figure out
        return ""
        # raise NotImplementedError()
        # return util._expect_field_type(self.jobj, "schemaUrl", str, optional=True)


class SS4OResourceSpanCollection(base.ResourceSpanCollection):
    def __init__(
        self, resource: SS4OResource, search_results: Iterator[util.JSONLikeDict]
    ):
        self.resource = resource
        self.search_results = search_results

    @property
    def otlp_resource(self) -> SS4OResource:
        return self.resource

    @property
    def otlp_scope_spans(self) -> Iterator[SS4OScopeSpanCollection]:
        return (
            SS4OScopeSpanCollection(SS4OInstrumentationScope(scope), spans)
            for scope, spans in groupby(
                self.search_results, key=lambda x: x["instrumentationScope"]
            )
        )

    @property
    def otlp_schema_url(self) -> str:
        return ""
        ## TODO: Figure out
        ###raise NotImplementedError()


class SS4OSpanCollection(base.SpanCollection):
    def __init__(self, search_results: List[util.JSONLikeDict]):
        self.search_results = search_results

    @property
    def otlp_resource_spans(self) -> Iterator[SS4OResourceSpanCollection]:
        return (
            SS4OResourceSpanCollection(SS4OResource(resource), spans)
            for resource, spans in groupby(
                self.search_results, key=lambda x: x["resource"]
            )
        )
