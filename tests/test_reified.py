import python_opentelemetry_access.base as base
import python_opentelemetry_access.util as util


def test_roundtrip_complete_proto():
    field_timestamp_start = 21412
    field_timestamp_end = 39324
    field_name = "some name"
    field_attributes = {"foo": 3.2, "other": ["bar", 2], "stuff": {"inner": False}}
    field_attributes_kvlist = [
        {"key": "foo", "value": {"doubleValue": 3.2}},
        {
            "key": "other",
            "value": {"arrayValue": [{"stringValue": "bar"}, {"intValue": "2"}]},
        },
        {
            "key": "stuff",
            "value": {"kvlistValue": [{"key": "inner", "value": {"boolValue": False}}]},
        },
    ]
    field_dropped_attributes_count = 1
    field_trace_id = "ABC123"
    field_span_id = "DEF456"
    field_other_span_id = "FED654"
    field_trace_state = "some state"
    field_flags = 123
    field_version = "v1.2.3"

    span_event = base.ReifiedSpanEvent(
        time_unix_nano=field_timestamp_start,
        name=field_name,
        attributes=field_attributes,
        dropped_attributes_count=field_dropped_attributes_count,
    )
    assert util.force_jsonlike_dict_iter(span_event.to_otlp_json_iter()) == {
        "timeUnixNano": str(field_timestamp_start),
        "name": field_name,
        "attributes": field_attributes_kvlist,
        "droppedAttributesCount": field_dropped_attributes_count,
    }

    status = base.ReifiedStatus(message="some message", code=1)
    assert util.force_jsonlike_iter(status.to_otlp_json_iter()) == {
        "message": "some message",
        "code": 1,
    }

    field_span_kind = base.ReifiedSpanKind(5)
    assert util.force_jsonlike_iter(field_span_kind.to_otlp_json_iter()) == 5

    event1 = base.ReifiedSpanEvent(
        time_unix_nano=field_timestamp_start,
        name=field_name,
        attributes=field_attributes,
        dropped_attributes_count=field_dropped_attributes_count,
    )
    assert util.force_jsonlike_iter(event1.to_otlp_json_iter()) == {
        "timeUnixNano": str(field_timestamp_start),
        "name": field_name,
        "attributes": field_attributes_kvlist,
        "droppedAttributesCount": field_dropped_attributes_count,
    }
    field_events = [event1]

    field_status = base.ReifiedStatus(message="some message", code=2)
    assert util.force_jsonlike_iter(field_status.to_otlp_json_iter()) == {
        "code": 2,
        "message": "some message",
    }

    span = base.ReifiedSpan(
        trace_id=field_trace_id,
        span_id=field_span_id,
        trace_state=field_trace_state,
        parent_span_id=field_other_span_id,
        flags=field_flags,
        name=field_name,
        kind=field_span_kind,
        start_time_unix_nano=field_timestamp_start,
        end_time_unix_nano=field_timestamp_end,
        attributes=field_attributes,
        dropped_attributes_count=field_dropped_attributes_count,
        events=field_events,
        dropped_events_count=1,
        status=field_status,
        ## TODO: Add links to tests
        links=[],
        dropped_links_count=1,
    )
    assert util.force_jsonlike_iter(span.to_otlp_json_iter()) == {
        "traceId": field_trace_id,
        "spanId": field_span_id,
        "traceState": field_trace_state,
        "parentSpanId": field_other_span_id,
        "flags": field_flags,
        "name": field_name,
        "kind": util.force_jsonlike_iter(field_span_kind.to_otlp_json_iter()),
        "startTimeUnixNano": str(field_timestamp_start),
        "endTimeUnixNano": str(field_timestamp_end),
        "attributes": field_attributes_kvlist,
        "droppedAttributesCount": field_dropped_attributes_count,
        "events": [
            util.force_jsonlike_iter(e.to_otlp_json_iter()) for e in field_events
        ],
        "status": util.force_jsonlike_iter(field_status.to_otlp_json_iter()),
    }

    instrumentation_scope = base.ReifiedInstrumentationScope(
        name=field_name,
        version=field_version,
        attributes=field_attributes,
        dropped_attributes_count=field_dropped_attributes_count,
    )
    assert util.force_jsonlike_iter(instrumentation_scope.to_otlp_json_iter()) == {
        "name": field_name,
        "version": field_version,
        "attributes": field_attributes_kvlist,
        "droppedAttributesCount": field_dropped_attributes_count,
    }

    resource_ = base.ReifiedResource(
        attributes=field_attributes,
        dropped_attributes_count=field_dropped_attributes_count,
    )
    assert util.force_jsonlike_iter(resource_.to_otlp_json_iter()) == {
        "attributes": field_attributes_kvlist,
        "droppedAttributesCount": field_dropped_attributes_count,
    }

    scope_span_collection = base.ReifiedScopeSpanCollection(
        scope=instrumentation_scope, spans=[span], schema_url=None
    )
    assert util.force_jsonlike_iter(scope_span_collection.to_otlp_json_iter()) == {
        "scope": util.force_jsonlike_iter(instrumentation_scope.to_otlp_json_iter()),
        "spans": [util.force_jsonlike_iter(ss.to_otlp_json_iter()) for ss in [span]],
    }

    resource_span_collection = base.ReifiedResourceSpanCollection(
        resource=resource_, scope_spans=[scope_span_collection], schema_url=None
    )
    assert util.force_jsonlike_iter(resource_span_collection.to_otlp_json_iter()) == {
        "resource": util.force_jsonlike_iter(resource_.to_otlp_json_iter()),
        "scopeSpans": [
            util.force_jsonlike_iter(ssc.to_otlp_json_iter())
            for ssc in [scope_span_collection]
        ],
    }

    span_collection = base.ReifiedSpanCollection(
        resource_spans=[resource_span_collection]
    )
    assert util.force_jsonlike_iter(span_collection.to_otlp_json_iter()) == {
        "resourceSpans": [
            util.force_jsonlike_iter(rsc.to_otlp_json_iter())
            for rsc in [resource_span_collection]
        ]
    }
