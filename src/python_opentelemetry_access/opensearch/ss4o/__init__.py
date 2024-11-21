from collections.abc import Iterator, Generator
from typing import Optional, TextIO
from threading import Lock
import json


from itertools import groupby

from ... import base
from ... import util

## Only needed for correctly parsing nanosecond timestamps
from pandas import Timestamp
import opentelemetry_betterproto.opentelemetry.proto.trace.v1 as trace


def _parse_ns_isotime(t: str) -> int:
    return int(Timestamp(t).asm8.astype("datetime64[ns]"))


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
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return util.iter_jsonlike_dict(
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
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return util.iter_jsonlike_dict(
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
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return util.iter_jsonlike_dict(
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
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return util.iter_jsonlike_dict(
            util._expect_field_type(
                self.jobj, "attributes", dict, optional=True, default={}
            )
        )

    @property
    def otlp_dropped_attributes_count(self) -> int:
        ## TODO: Need to figure out how this is handled
        return 0


class SS4OResource(base.Resource):
    def __init__(self, jobj: util.JSONLikeDict):
        self.jobj = jobj

    @property
    def otlp_attributes_iter(self) -> util.JSONLikeDictIter:
        return util.iter_jsonlike_dict(self.jobj)

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
        self._lock = Lock()
        self._invalidated = False

    @property
    def otlp_scope(self) -> SS4OInstrumentationScope:
        return self.scope

    @property
    def otlp_spans(self) -> Iterator[SS4OSpan]:
        fail = False
        with self._lock:
            if self._invalidated:
                fail = True
            else:
                self._invalidated = True
        if fail:
            raise RuntimeError("Invalidated ScopeSpanCollection")

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
        self._lock = Lock()
        self._invalidated = False

    @property
    def otlp_resource(self) -> SS4OResource:
        return self.resource

    @property
    def otlp_scope_spans(self) -> Iterator[SS4OScopeSpanCollection]:
        fail = False
        with self._lock:
            if self._invalidated:
                fail = True
            else:
                self._invalidated = True
        if fail:
            raise RuntimeError("Invalidated ResourceSpanCollection")

        def inner():
            for scope, spans in groupby(
                self.search_results, key=lambda x: x["instrumentationScope"]
            ):
                new = SS4OScopeSpanCollection(SS4OInstrumentationScope(scope), spans)
                yield new
                new._invalidated = True

        return inner()

    @property
    def otlp_schema_url(self) -> str:
        return ""
        ## TODO: Figure out
        ###raise NotImplementedError()


def _iter_bare_results(
    search_results: util.JSONLikeList,
) -> Generator[SS4OResourceSpanCollection]:
    for resource, spans in groupby(
        search_results,
        key=lambda x: util.expect_dict(util.expect_dict(x)["resource"]),
    ):
        new = SS4OResourceSpanCollection(
            SS4OResource(resource), (util.expect_dict(span) for span in spans)
        )
        yield new
        new._invalidated = True


def _iter_full_results(
    search_results: util.JSONLikeDict,
) -> Generator[SS4OResourceSpanCollection]:
    for resource, spans in groupby(
        util.expect_list(
            util.expect_dict(util.expect_dict(search_results)["hits"])["hits"]
        ),
        key=lambda x: util.expect_dict(util.expect_dict(x)["resource"]),
    ):
        new = SS4OResourceSpanCollection(
            SS4OResource(resource),
            (util.expect_dict(util.expect_dict(result)["_source"]) for result in spans),
        )
        yield new
        new._invalidated = True


class SS4OSpanCollection(base.SpanCollection):
    def __init__(self, search_results: util.JSONLike):
        self._search_results = search_results

    @property
    def otlp_resource_spans(self) -> Iterator[SS4OResourceSpanCollection]:
        return _iter_full_results(util.expect_dict(self._search_results))


class SS4OSpanCollectionBare(base.SpanCollection):
    def __init__(self, search_results: util.JSONLike):
        self._search_results = search_results

    @property
    def otlp_resource_spans(self) -> Iterator[SS4OResourceSpanCollection]:
        return _iter_bare_results(util.expect_list(self._search_results))


def loado(o: util.JSONLike):
    return SS4OSpanCollection(o)


def loado_bare(o: util.JSONLike):
    return SS4OSpanCollectionBare(o)


def load(fp: TextIO):
    return loado(json.load(fp))


def load_bare(fp: TextIO):
    return loado_bare(json.load(fp))


def loads(s: str):
    return loado(json.loads(s))


def loads_bare(s: str):
    return loado_bare(json.loads(s))
