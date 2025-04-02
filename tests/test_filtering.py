from dataclasses import dataclass
from datetime import datetime

from pytest import mark
from python_opentelemetry_access import otlpjson, util
from python_opentelemetry_access.proxy import _filter_span_collection


@dataclass
class Params:
    expected_spans: list[str]
    from_time: datetime | None = None
    to_time: datetime | None = None
    span_ids: list[tuple[str | None, str | None]] | None = None
    resource_attributes: util.AttributesFilter | None = None
    scope_attributes: util.AttributesFilter | None = None
    span_attributes: util.AttributesFilter | None = None
    span_name: str | None = None


@mark.parametrize(
    "params",
    [
        Params(
            expected_spans=["res1_scope1_trace1_span1", "res1_scope2_trace1_span1"],
            span_name="some_span1",
        ),
        Params(
            expected_spans=["res1_scope1_trace1_span1", "res1_scope2_trace1_span1"],
            resource_attributes={
                "int_resource_attr": [100],
                "string_resource_attr": None,
            },
            span_attributes={"string_span_attr": ["span string 1"]},
        ),
        Params(
            expected_spans=[],
            resource_attributes={
                "int_resource_attr": [100],
                "string_resource_attr_typo": None,
            },
            span_attributes={"string_span_attr": ["span string 1"]},
        ),
        Params(
            expected_spans=["res1_scope1_trace1_span1", "res1_scope2_trace1_span1"],
            resource_attributes={"int_resource_attr": [100]},
            span_attributes={"string_span_attr": ["span string 1"]},
        ),
        Params(
            expected_spans=[
                "res1_scope1_trace1_span1",
                "res1_scope1_trace1_span2",
                "res2_scope1_trace1_span1",
            ],
            resource_attributes={
                "int_resource_attr": [100, 200],
                "string_resource_attr": ["resource string 1", "resource string 2"],
            },
            scope_attributes={"string_scope_attr": ["scope string 1"]},
        ),
        Params(
            expected_spans=[],
            resource_attributes={"string_resource_attr": ["invalid_value"]},
        ),
        Params(
            expected_spans=[],
            span_attributes={"invalid_attribute": ["span string 2"]},
        ),
    ],
)
def test_filtering(params: Params) -> None:
    filtered_spans = _filter_span_collection(
        otlpjson.loado(_get_spans()).to_reified(),
        from_time=params.from_time,
        to_time=params.to_time,
        span_ids=params.span_ids,
        resource_attributes=params.resource_attributes,
        scope_attributes=params.scope_attributes,
        span_attributes=params.span_attributes,
        span_name=params.span_name,
    )

    span_ids = set(
        span.to_reified().span_id for _, _, span in filtered_spans.iter_spans()
    )

    print(span_ids)

    assert span_ids == set(params.expected_spans)


def _get_spans() -> util.JSONLike:
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "int_resource_attr", "value": {"intValue": "100"}},
                        {
                            "key": "string_resource_attr",
                            "value": {"stringValue": "resource string 1"},
                        },
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {
                            "name": "some_scope",
                            "attributes": [
                                {"key": "int_scope_attr", "value": {"intValue": "10"}},
                                {
                                    "key": "string_scope_attr",
                                    "value": {"stringValue": "scope string 1"},
                                },
                            ],
                        },
                        "spans": [
                            {
                                "traceId": "res1_scope1_trace1",
                                "spanId": "res1_scope1_trace1_span1",
                                "parentSpanId": "",
                                "startTimeUnixNano": "1729007194857023153",
                                "endTimeUnixNano": "1729007194857127603",
                                "name": "some_span1",
                                "kind": 1,
                                "status": {},
                                "attributes": [
                                    {
                                        "key": "int_span_attr",
                                        "value": {"intValue": "1"},
                                    },
                                    {
                                        "key": "string_span_attr",
                                        "value": {"stringValue": "span string 1"},
                                    },
                                ],
                            },
                            {
                                "traceId": "res1_scope1_trace1",
                                "spanId": "res1_scope1_trace1_span2",
                                "parentSpanId": "res1_scope1_trace1_span1",
                                "startTimeUnixNano": "1729007194857023153",
                                "endTimeUnixNano": "1729007194857127603",
                                "name": "some_span2",
                                "kind": 1,
                                "status": {},
                                "attributes": [
                                    {
                                        "key": "int_span_attr",
                                        "value": {"intValue": "2"},
                                    },
                                    {
                                        "key": "string_span_attr",
                                        "value": {"stringValue": "span string 2"},
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "scope": {
                            "name": "some_scope",
                            "attributes": [
                                {"key": "int_scope_attr", "value": {"intValue": "20"}},
                                {
                                    "key": "string_scope_attr",
                                    "value": {"stringValue": "scope string 2"},
                                },
                            ],
                        },
                        "spans": [
                            {
                                "traceId": "res1_scope2_trace1",
                                "spanId": "res1_scope2_trace1_span1",
                                "parentSpanId": "",
                                "startTimeUnixNano": "1729007194857023153",
                                "endTimeUnixNano": "1729007194857127603",
                                "name": "some_span1",
                                "kind": 1,
                                "status": {},
                                "attributes": [
                                    {
                                        "key": "int_span_attr",
                                        "value": {"intValue": "1"},
                                    },
                                    {
                                        "key": "string_span_attr",
                                        "value": {"stringValue": "span string 1"},
                                    },
                                ],
                            },
                            {
                                "traceId": "res1_scope2_trace1",
                                "spanId": "res1_scope2_trace1_span2",
                                "parentSpanId": "res1_scope2_trace1_span1",
                                "startTimeUnixNano": "1729007194857023153",
                                "endTimeUnixNano": "1729007194857127603",
                                "name": "some_span2",
                                "kind": 1,
                                "status": {},
                                "attributes": [
                                    {
                                        "key": "int_span_attr",
                                        "value": {"intValue": "2"},
                                    },
                                    {
                                        "key": "string_span_attr",
                                        "value": {"stringValue": "span string 2"},
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
            {
                "resource": {
                    "attributes": [
                        {"key": "int_resource_attr", "value": {"intValue": "200"}},
                        {
                            "key": "string_resource_attr",
                            "value": {"stringValue": "resource string 2"},
                        },
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {
                            "name": "some_scope",
                            "attributes": [
                                {"key": "int_scope_attr", "value": {"intValue": "10"}},
                                {
                                    "key": "string_scope_attr",
                                    "value": {"stringValue": "scope string 1"},
                                },
                            ],
                        },
                        "spans": [
                            {
                                "traceId": "res2_scope1_trace1",
                                "spanId": "res2_scope1_trace1_span1",
                                "parentSpanId": "",
                                "startTimeUnixNano": "1729007194857023153",
                                "endTimeUnixNano": "1729007194857127603",
                                "name": "some_span3",
                                "kind": 1,
                                "status": {},
                                "attributes": [
                                    {
                                        "key": "int_span_attr",
                                        "value": {"intValue": "1"},
                                    },
                                    {
                                        "key": "string_span_attr",
                                        "value": {"stringValue": "span string 1"},
                                    },
                                ],
                            },
                        ],
                    }
                ],
            },
        ]
    }
