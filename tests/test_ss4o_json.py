# import json

import python_opentelemetry_access.util as util
import python_opentelemetry_access.opensearch_ss4o as opensearch_ss4o
from pandas import Timestamp


def test_roundtrip_complete_json():
    field_timestamp_start = "2024-10-15T15:46:34.857023153Z"
    field_timestamp_end = "2024-10-15T15:46:34.857127603Z"
    field_name = "some name"
    field_attributes = {"foo": 3.2, "other": ["bar", 2], "stuff": {"inner": False}}
    field_dropped_attributes_count = 1
    field_trace_id = "ABC123"
    field_span_id = "DEF456"
    field_other_span_id = "FED654"
    field_trace_state = "some state"
    field_flags = 123
    field_version = "v1.2.3"

    span_event = {
        "@timestamp": field_timestamp_start,
        "name": field_name,
        "attributes": field_attributes,
        "droppedAttributesCount": field_dropped_attributes_count,
    }

    ss4o_span_event = opensearch_ss4o.SS4OSpanEvent(span_event)
    assert (
        Timestamp(ss4o_span_event.otlp_time_unix_nano).isoformat() + "Z"
        == field_timestamp_start
    )
    assert ss4o_span_event.otlp_name == field_name
    assert (
        ss4o_span_event.otlp_dropped_attributes_count == field_dropped_attributes_count
    )
    assert (
        util.force_jsonlike_dict_iter(ss4o_span_event.otlp_attributes_iter)
        == field_attributes
    )

    # ## TODO: Check that this is the correct format
    field_message = "some message"
    field_status = {"message": field_message, "code": "Unset"}
    ss4o_status = opensearch_ss4o.SS4OStatus(field_status)
    assert ss4o_status.otlp_message == field_message
    assert ss4o_status.otlp_code == 0

    field_span_kind = "Internal"
    ss4o_span_kind = opensearch_ss4o.SS4OSpanKind(field_span_kind)
    assert ss4o_span_kind.otlp_kind_name == "SPAN_KIND_INTERNAL"

    field_events = [span_event]

    span = {
        "traceId": field_trace_id,
        "spanId": field_span_id,
        "traceState": field_trace_state,
        "parentSpanId": field_other_span_id,
        "flags": field_flags,
        "name": field_name,
        "kind": field_span_kind,
        "startTime": field_timestamp_start,
        "endTime": field_timestamp_end,
        "attributes": field_attributes,
        "droppedAttributesCount": field_dropped_attributes_count,
        "events": field_events,
        "status": field_status,
    }

    ss4o_span = opensearch_ss4o.SS4OSpan(span)
    assert ss4o_span.otlp_trace_id == field_trace_id
    assert ss4o_span.otlp_span_id == field_span_id
    assert ss4o_span.otlp_trace_state == field_trace_state
    assert ss4o_span.otlp_parent_span_id == field_other_span_id
    assert ss4o_span.otlp_flags == field_flags
    assert ss4o_span.otlp_name == field_name
    assert ss4o_span.otlp_kind.otlp_kind_name == "SPAN_KIND_INTERNAL"
    assert (
        Timestamp(ss4o_span.otlp_start_time_unix_nano).isoformat() + "Z"
        == field_timestamp_start
    )
    assert (
        Timestamp(ss4o_span.otlp_end_time_unix_nano).isoformat() + "Z"
        == field_timestamp_end
    )
    assert util.force_jsonlike_dict_iter(ss4o_span.otlp_attributes_iter) == field_attributes
    assert ss4o_span.otlp_dropped_attributes_count == field_dropped_attributes_count
    assert next(ss4o_span.otlp_events).to_otlp_json() == ss4o_span_event.to_otlp_json()
    assert ss4o_span.otlp_status.to_otlp_json() == ss4o_status.to_otlp_json()

    instrumentation_scope = {
        "name": field_name,
        "version": field_version,
        "attributes": field_attributes,
        ## Not sure how to handle this in OpenSearch
        # "droppedAttributesCount": field_dropped_attributes_count,
    }
    ss4o_instrumentation_scope = opensearch_ss4o.SS4OInstrumentationScope(
        instrumentation_scope
    )
    assert ss4o_instrumentation_scope.otlp_name == field_name
    assert ss4o_instrumentation_scope.otlp_version == field_version
    assert (
        util.force_jsonlike_dict_iter(ss4o_instrumentation_scope.otlp_attributes_iter)
        == field_attributes
    )

    resource_fields_flat = {
        "foo.i": 3,
        "foo.s": "hello",
        "foo.b": True,
        "bar": 4,
        "baz.foo.i": 5,
        "baz.foo.s": "hej",
        "baz.foo.b": False,
        "baz.bar": 6,
    }
    resource_fields = {
        "foo": {
            "i": 3,
            "s": "hello",
            "b": True,
        },
        "bar": 4,
        "baz": {
            "foo": {
                "i": 5,
                "s": "hej",
                "b": False,
            },
            "bar": 6,
        },
    }
    resource = {
        "attributes": resource_fields_flat,
        ## Not sure how to handle this in OpenSearch yet
        # "droppedAttributesCount": field_dropped_attributes_count,
    }
    ss4o_resource = opensearch_ss4o.SS4OResource(resource)
    assert (
        util.force_jsonlike_dict_iter(ss4o_resource.otlp_attributes_iter) == resource_fields
    )

    search_results = [
        dict(
            **span,
            **{"instrumentationScope": instrumentation_scope, "resource": resource},
        )
    ]

    ss4o_span_collection = opensearch_ss4o.SS4OSpanCollectionBare(search_results)

    ss4o_resource_span_collection = next(ss4o_span_collection.otlp_resource_spans)
    assert (
        ss4o_resource_span_collection.otlp_resource.to_otlp_json()
        == ss4o_resource.to_otlp_json()
    )

    ss4o_scope_collection = next(ss4o_resource_span_collection.otlp_scope_spans)
    assert (
        ss4o_scope_collection.otlp_scope.to_otlp_json()
        == ss4o_instrumentation_scope.to_otlp_json()
    )

    tmp_span = next(ss4o_scope_collection.otlp_spans)
    assert tmp_span.to_otlp_json() == ss4o_span.to_otlp_json()
