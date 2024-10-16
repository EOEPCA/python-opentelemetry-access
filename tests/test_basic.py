import json

import opentelemetry_betterproto.opentelemetry.proto.common.v1 as common
import opentelemetry_betterproto.opentelemetry.proto.collector.trace.v1 as trace_collector

from python_opentelemetry_access import to_otlp_jsonlike

from pytest import mark

@mark.parametrize(
    "proto_rep_path, json_rep_path",
    [
        ('tests/examples/ex1.binpb', 'tests/examples/ex1.json'), ('tests/examples/ex2.binpb', 'tests/examples/ex2.json')
    ],
)
def test_example_trace(proto_rep_path: str, json_rep_path: str):
    with open(proto_rep_path, 'rb') as f:
        parsed_binary = trace_collector.ExportTraceServiceRequest().parse(f.read())
    with open(json_rep_path, 'r') as f:
        expected_json = json.load(f)

    # with open(f'tmp_{json_rep_path}_bin.json'.replace("/", "__"), 'w') as f:
    #     json.dump(to_otlp_jsonlike(parsed_binary), f, indent=2)
    # with open(f'tmp_{json_rep_path}_orig.json'.replace("/", "__"), 'w') as f:
    #     json.dump(expected_json, f, indent=2)
    
    assert expected_json == to_otlp_jsonlike(parsed_binary)


def test_unit_anytype():
    # assert to_otlp_jsonlike(common.KeyValueList([common.KeyValue("SomeKey", value=common.AnyValue(string_value = "SomeStringValue"))])) == { "SomeKey" : "SomeStringValue" }
    # assert to_otlp_jsonlike(common.KeyValueList([common.KeyValue("SomeKey", value=common.AnyValue(bool_value=True))])) == { "SomeKey" : True }
    # assert to_otlp_jsonlike(common.KeyValueList([common.KeyValue("SomeKey", value=common.AnyValue(int_value=4))])) == { "SomeKey" : 4 }
    # assert to_otlp_jsonlike(common.KeyValueList([common.KeyValue("SomeKey", value=common.AnyValue(double_value=4.2))])) == { "SomeKey" : 4.2 }
    # assert to_otlp_jsonlike(common.KeyValueList([common.KeyValue("SomeKey", value=common.AnyValue(array_value=common.ArrayValue()))])) == { "SomeKey" : [] }
    # assert to_otlp_jsonlike(common.KeyValueList([common.KeyValue("SomeKey", value=common.AnyValue(kvlist_value=common.KeyValueList([])))])) == { "SomeKey" : {} }
    assert to_otlp_jsonlike(
        common.KeyValueList(
            [
                common.KeyValue(
                    "SomeKey", value=common.AnyValue(string_value="SomeStringValue")
                )
            ]
        )
    ) == [{"key": "SomeKey", "value": {"stringValue": "SomeStringValue"}}]
    assert to_otlp_jsonlike(
        common.KeyValueList(
            [common.KeyValue("SomeKey", value=common.AnyValue(bool_value=True))]
        )
    ) == [{"key": "SomeKey", "value": {"boolValue": True}}]
    assert to_otlp_jsonlike(
        common.KeyValueList(
            [common.KeyValue("SomeKey", value=common.AnyValue(int_value=4))]
        )
    ) == [{"key": "SomeKey", "value": {"intValue": "4"}}]
    assert to_otlp_jsonlike(
        common.KeyValueList(
            [common.KeyValue("SomeKey", value=common.AnyValue(double_value=4.2))]
        )
    ) == [{"key": "SomeKey", "value": {"doubleValue": 4.2}}]
    assert to_otlp_jsonlike(
        common.KeyValueList(
            [
                common.KeyValue(
                    "SomeKey", value=common.AnyValue(array_value=common.ArrayValue())
                )
            ]
        )
    ) == [{"key": "SomeKey", "value": {"arrayValue": []}}]
    assert to_otlp_jsonlike(
        common.KeyValueList(
            [
                common.KeyValue(
                    "SomeKey",
                    value=common.AnyValue(kvlist_value=common.KeyValueList([])),
                )
            ]
        )
    ) == [{"key": "SomeKey", "value": {"kvlistValue": []}}]

    # assert to_otlp_jsonlike(common.KeyValueList([
    #     common.KeyValue("SomeKey",value=common.AnyValue(kvlist_value=common.KeyValueList([]))),
    #     common.KeyValue("SomeMoreKey",value=common.AnyValue(kvlist_value=common.KeyValueList([])))
    #     ])) == { "SomeKey" : {}, "SomeMoreKey" : {} }
    assert to_otlp_jsonlike(
        common.KeyValueList(
            [
                common.KeyValue(
                    "SomeKey",
                    value=common.AnyValue(kvlist_value=common.KeyValueList([])),
                ),
                common.KeyValue(
                    "SomeMoreKey",
                    value=common.AnyValue(kvlist_value=common.KeyValueList([])),
                ),
            ]
        )
    ) == [
        {"key": "SomeKey", "value": {"kvlistValue": []}},
        {"key": "SomeMoreKey", "value": {"kvlistValue": []}},
    ]

    # assert to_otlp_jsonlike(common.KeyValueList([
    #     common.KeyValue("SomeKey",value=common.AnyValue(kvlist_value=common.KeyValueList([
    #         common.KeyValue("SomeInnerKey",value=common.AnyValue(kvlist_value=common.KeyValueList([])))
    #     ])))
    #     ])) == { "SomeKey" : { "SomeInnerKey" : {} } }
    assert to_otlp_jsonlike(
        common.KeyValueList(
            [
                common.KeyValue(
                    "SomeKey",
                    value=common.AnyValue(
                        kvlist_value=common.KeyValueList(
                            [
                                common.KeyValue(
                                    "SomeInnerKey",
                                    value=common.AnyValue(
                                        kvlist_value=common.KeyValueList([])
                                    ),
                                )
                            ]
                        )
                    ),
                )
            ]
        )
    ) == [
        {
            "key": "SomeKey",
            "value": {
                "kvlistValue": [{"key": "SomeInnerKey", "value": {"kvlistValue": []}}]
            },
        }
    ]

    # assert to_otlp_jsonlike(common.KeyValueList([
    #     common.KeyValue("SomeKey", value=common.AnyValue(array_value=common.ArrayValue([common.AnyValue(int_value=4)])))
    #     ])) == { "SomeKey" : [4] }
    assert to_otlp_jsonlike(
        common.KeyValueList(
            [
                common.KeyValue(
                    "SomeKey",
                    value=common.AnyValue(
                        array_value=common.ArrayValue([common.AnyValue(int_value=4)])
                    ),
                )
            ]
        )
    ) == [{"key": "SomeKey", "value": {"arrayValue": [{"intValue": "4"}]}}]
