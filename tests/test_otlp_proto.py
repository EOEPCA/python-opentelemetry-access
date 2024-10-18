import binascii

# import python_opentelemetry_access.base as base
import python_opentelemetry_access.otlpproto as otlpproto

import opentelemetry_betterproto.opentelemetry.proto.common.v1 as common
import opentelemetry_betterproto.opentelemetry.proto.trace.v1 as trace

import opentelemetry_betterproto.opentelemetry.proto.collector.trace.v1 as trace_collector
import opentelemetry_betterproto.opentelemetry.proto.resource.v1 as resource


def test_roundtrip_complete_proto():
    field_timestamp_start = 21412
    field_timestamp_end = 39324
    field_name = "some name"
    field_attributes = [
        common.KeyValue("key", common.AnyValue(double_value=3.2)),
        common.KeyValue(
            "other",
            common.AnyValue(
                array_value=common.ArrayValue(
                    [
                        common.AnyValue(string_value="bar"),
                        common.AnyValue(int_value=2),
                    ]
                )
            ),
        ),
        common.KeyValue(
            "stuff",
            common.AnyValue(
                kvlist_value=common.KeyValueList(
                    [common.KeyValue("inner", common.AnyValue(bool_value=False))]
                )
            ),
        ),
    ]
    field_dropped_attributes_count = 1
    field_trace_id = binascii.a2b_hex("ABC123")
    field_span_id = binascii.a2b_hex("DEF456")
    field_other_span_id = binascii.a2b_hex("FED654")
    field_trace_state = "some state"
    field_flags = 123
    field_version = "v1.2.3"

    span_event = trace.SpanEvent(
        time_unix_nano=field_timestamp_start,
        name=field_name,
        attributes=field_attributes,
        dropped_attributes_count=field_dropped_attributes_count,
    )

    # # useful for debugging
    # import pprint
    #
    # with open("tmp_span_event_bin.json".replace("/", "__"), "w") as f:
    #     pprint.pprint(span_event, f, indent=2)
    # with open("tmp_span_event_reproc.json".replace("/", "__"), "w") as f:
    #     pprint.pprint(
    #         otlpproto.OTLPProtoSpanEvent(span_event).to_otlp_protobuf(), f, indent=2
    #     )

    assert span_event == otlpproto.OTLPProtoSpanEvent(span_event).to_otlp_protobuf()

    status = trace.Status(message="some message", code=1)
    assert status == otlpproto.OTLPProtoStatus(status).to_otlp_protobuf()

    field_span_kind = 5
    assert (
        field_span_kind
        == otlpproto.OTLPProtoSpanKind(field_span_kind).to_otlp_protobuf()
    )

    event1 = trace.SpanEvent(
        time_unix_nano=field_timestamp_start,
        name=field_name,
        attributes=field_attributes,
        dropped_attributes_count=field_dropped_attributes_count,
    )
    assert event1 == otlpproto.OTLPProtoSpanEvent(event1).to_otlp_protobuf()
    field_events = [event1]

    field_status = trace.Status(message="some message", code=trace.StatusStatusCode(2))
    assert field_status == otlpproto.OTLPProtoStatus(field_status).to_otlp_protobuf()

    span = trace.Span(
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
        status=field_status,
    )
    assert span == otlpproto.OTLPProtoSpan(span).to_otlp_protobuf()

    instrumentation_scope = common.InstrumentationScope(
        name=field_name,
        version=field_version,
        attributes=field_attributes,
        dropped_attributes_count=field_dropped_attributes_count,
    )
    assert (
        instrumentation_scope
        == otlpproto.OTLPProtoInstrumentationScope(
            instrumentation_scope
        ).to_otlp_protobuf()
    )

    resource_ = resource.Resource(
        attributes=field_attributes,
        dropped_attributes_count=field_dropped_attributes_count,
    )
    assert resource_ == otlpproto.OTLPProtoResource(resource_).to_otlp_protobuf()

    scope_span_collection = trace.ScopeSpans(scope=instrumentation_scope, spans=[span])
    assert (
        scope_span_collection
        == otlpproto.OTLPProtoScopeSpanCollection(
            scope_span_collection
        ).to_otlp_protobuf()
    )

    resource_span_collection = trace.ResourceSpans(
        resource=resource_, scope_spans=[scope_span_collection]
    )
    assert (
        resource_span_collection
        == otlpproto.OTLPProtoResourceSpanCollection(
            resource_span_collection
        ).to_otlp_protobuf()
    )

    span_collection = trace_collector.ExportTraceServiceRequest(
        resource_spans=[resource_span_collection]
    )
    assert (
        span_collection
        == otlpproto.OTLPProtoSpanCollection(span_collection).to_otlp_protobuf()
    )
