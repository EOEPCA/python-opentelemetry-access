from typing import List, Dict, Union, Any
import binascii

import opentelemetry_betterproto.opentelemetry.proto.common.v1 as common
import opentelemetry_betterproto.opentelemetry.proto.trace.v1 as trace
import opentelemetry_betterproto.opentelemetry.proto.collector.trace.v1 as trace_collector
import opentelemetry_betterproto.opentelemetry.proto.resource.v1 as resource

JSONLike = Union[str, bool, int, float, List["JSONLike"], Dict[str, "JSONLike"]]

## NOTE: This is required since OTLP-JSON does not exactly match standard serialisation
def to_otlp_jsonlike(self: Any) -> JSONLike:
    """Convert data into a structure compatible with https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding"""
    match self:
        case common.AnyValue(string_value=str_val):
            # return str_val
            return {"stringValue": str_val}
        case common.AnyValue(bool_value=bool_val):
            # return bool_val
            return {"boolValue": bool_val}
        case common.AnyValue(int_value=int_val):
            # return int_val
            return {"intValue": str(int_val)}
        case common.AnyValue(double_value=double_val):
            # return double_val
            return {"doubleValue": double_val}
        case common.AnyValue(array_value=xs):
            # return to_otlp_jsonlike(xs)
            return {"arrayValue": to_otlp_jsonlike(xs)}
        case common.AnyValue(kvlist_value=kvlist_value):
            # return to_otlp_jsonlike(kvlist_value)
            return {"kvlistValue": to_otlp_jsonlike(kvlist_value)}
        case common.AnyValue(bytes_value=_):
            raise RuntimeError("_to_otlp_jsonlike__AnyValue: Bytes not allowed here")
        case common.KeyValueList(kvs):
            # return {
            #    kv.key : to_otlp_jsonlike(kv.value)
            #    for kv in kvs
            # }
            return [{"key": kv.key, "value": to_otlp_jsonlike(kv.value)} for kv in kvs]
        case common.ArrayValue(xs):
            return [to_otlp_jsonlike(x) for x in xs]
        case trace_collector.ExportTraceServiceRequest(resource_spans=resource_spanss):
            return {
                "resourceSpans": [
                    to_otlp_jsonlike(resource_spans)
                    for resource_spans in resource_spanss
                ]
            }
        case trace.ResourceSpans(resource=resource_, scope_spans=scope_spanss):
            return {
                "resource": to_otlp_jsonlike(resource_),
                "scopeSpans": [
                    to_otlp_jsonlike(scope_spans) for scope_spans in scope_spanss
                ],
            }
        case trace.ScopeSpans(scope=scope, spans=spans):
            return {
                "scope": to_otlp_jsonlike(scope),
                "spans": [to_otlp_jsonlike(span) for span in spans],
            }
        ## TODO: Add dropped attribute count
        case common.InstrumentationScope(
            name=name, version=version, attributes=attributes
        ):
            return {
                "name": name,
                **({"version": version} if version else {}),
                **(
                    {"attributes": to_otlp_jsonlike(common.KeyValueList(attributes))}
                    if attributes
                    else {}
                ),
            }
        ## TODO: Dropped attributes?
        case trace.Span(
            trace_id=trace_id,
            span_id=span_id,
            trace_state=_,
            parent_span_id=parent_span_id,
            flags=flags,
            name=name,
            kind=kind,
            start_time_unix_nano=start_time_unix_nano,
            end_time_unix_nano=end_time_unix_nano,
            attributes=attributes,
            ## TODO: Beyond here
            dropped_attributes_count=_,
            events=events,
            dropped_events_count=_,
            links=_,
            dropped_links_count=_,
            status=status,
        ):
            return {
                "traceId": binascii.b2a_hex(trace_id).decode("ascii"),
                "spanId": binascii.b2a_hex(span_id).decode("ascii"),
                #'traceState': TODO
                "parentSpanId": binascii.b2a_hex(parent_span_id).decode("ascii"),
                **({"flags": flags} if flags != 0 else {}),
                "name": name,
                "kind": int(kind),
                "startTimeUnixNano": str(start_time_unix_nano),
                "endTimeUnixNano": str(end_time_unix_nano),
                **(
                    {"attributes": to_otlp_jsonlike(common.KeyValueList(attributes))}
                    if attributes
                    else {}
                ),
                ## TODO: Status message?
                **(
                    {"events": [to_otlp_jsonlike(event) for event in events]}
                    if events
                    else {}
                ),
                "status": to_otlp_jsonlike(status),
            }
        ## TODO: Dropped attributes?
        case trace.Status(message=message, code=code):
            return {
                **({"code": int(code)} if int(code) != 0 else {}),
                **({"message": message} if message else {}),
            }
        case trace.SpanEvent(
            time_unix_nano=time_unix_nano,
            name=name,
            attributes=attributes,
            dropped_attributes_count=_,
        ):
            return {
                "timeUnixNano": str(time_unix_nano),
                "name": name,
                **(
                    {"attributes": to_otlp_jsonlike(common.KeyValueList(attributes))}
                    if attributes
                    else {}
                ),
            }
        case resource.Resource(attributes=attributes):
            return (
                {"attributes": to_otlp_jsonlike(common.KeyValueList(attributes))}
                if attributes
                else {}
            )
        case x:
            raise NotImplementedError(f"to_otlp_jsonlike: not implemented for {x}")
