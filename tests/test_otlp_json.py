import json

# import python_opentelemetry_access.base as base
import python_opentelemetry_access.otlpjson as otlpjson


def test_roundtrip_complete_json():
    field_timestamp_start = "21412"
    field_timestamp_end = "39324"
    field_name = "some name"
    field_attributes = [
        {"key": "foo", "value": {"doubleValue": 3.2}},
        # {
        #     "key": "bar",
        #     "value": {
        #         "arrayValue": {"values": [{"stringValue": "bar"}, {"intValue": "2"}]}
        #     },
        # },
        {
            "key": "other",
            "value": {"arrayValue": {"values": [{"intValue": "3"}, {"intValue": "2"}]}},
        },
        # {
        #     "key": "stuff",
        #     "value": {
        #         "kvlistValue": [
        #             {"key": "inner", "value": {"boolValue": False}},
        #             {"key": "inner2", "value": {"intValue": "10"}},
        #         ]
        #     },
        # },
        {"key": "stuff.inner2", "value": {"boolValue": True}},
    ]
    field_dropped_attributes_count = 1
    field_trace_id = "ABC123"
    field_span_id = "DEF456"
    field_other_span_id = "FED654"
    field_trace_state = "some state"
    field_flags = 123
    field_version = "v1.2.3"

    span_event = {
        "timeUnixNano": field_timestamp_start,
        "name": field_name,
        "attributes": field_attributes,
        "droppedAttributesCount": field_dropped_attributes_count,
    }

    assert (
        json.dumps(span_event) == otlpjson.OTLPJsonSpanEvent(span_event).to_otlp_json()
    )

    ## TODO: Check that this is the correct format
    status = {"message": "some message", "code": 1}
    assert json.dumps(status) == otlpjson.OTLPJsonStatus(status).to_otlp_json()

    field_span_kind = 5
    assert (
        json.dumps(field_span_kind)
        == otlpjson.OTLPJsonSpanKind(field_span_kind).to_otlp_json()
    )

    event1 = {
        "timeUnixNano": field_timestamp_start,
        "name": field_name,
        "attributes": field_attributes,
        "droppedAttributesCount": field_dropped_attributes_count,
    }
    assert json.dumps(event1) == otlpjson.OTLPJsonSpanEvent(event1).to_otlp_json()
    field_events = [event1]

    field_status = {"message": "some status message", "code": 2}
    assert (
        json.dumps(field_status) == otlpjson.OTLPJsonStatus(field_status).to_otlp_json()
    )

    span = {
        "traceId": field_trace_id,
        "spanId": field_span_id,
        "traceState": field_trace_state,
        "parentSpanId": field_other_span_id,
        "flags": field_flags,
        "name": field_name,
        "kind": field_span_kind,
        "startTimeUnixNano": field_timestamp_start,
        "endTimeUnixNano": field_timestamp_end,
        "attributes": field_attributes,
        "droppedAttributesCount": field_dropped_attributes_count,
        "events": field_events,
        "status": field_status,
    }
    assert json.dumps(span) == otlpjson.OTLPJsonSpan(span).to_otlp_json()

    instrumentation_scope = {
        "name": field_name,
        "version": field_version,
        "attributes": field_attributes,
        "droppedAttributesCount": field_dropped_attributes_count,
    }
    assert (
        json.dumps(instrumentation_scope)
        == otlpjson.OTLPJsonInstrumentationScope(instrumentation_scope).to_otlp_json()
    )

    resource = {
        "attributes": field_attributes,
        "droppedAttributesCount": field_dropped_attributes_count,
    }
    assert json.dumps(resource) == otlpjson.OTLPJsonResource(resource).to_otlp_json()

    scope_span_collection = {"scope": instrumentation_scope, "spans": [span]}
    assert (
        json.dumps(scope_span_collection)
        == otlpjson.OTLPJsonScopeSpanCollection(scope_span_collection).to_otlp_json()
    )

    resource_span_collection = {
        "resource": resource,
        "scopeSpans": [scope_span_collection],
    }
    assert (
        json.dumps(resource_span_collection)
        == otlpjson.OTLPJsonResourceSpanCollection(
            resource_span_collection
        ).to_otlp_json()
    )

    span_collection = {"resourceSpans": [resource_span_collection]}
    assert (
        json.dumps(span_collection)
        == otlpjson.OTLPJsonSpanCollection(span_collection).to_otlp_json()
    )
