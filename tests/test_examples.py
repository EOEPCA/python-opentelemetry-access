import json

# import opentelemetry_betterproto.opentelemetry.proto.common.v1 as common
import opentelemetry_betterproto.opentelemetry.proto.collector.trace.v1 as trace_collector

import python_opentelemetry_access.otlpjson as otlpjson

from pytest import mark


@mark.parametrize(
    "proto_rep_path, json_rep_path",
    [
        ("tests/examples/ex1.binpb", "tests/examples/ex1.json"),
        ("tests/examples/ex2.binpb", "tests/examples/ex2.json"),
    ],
)
def test_example_trace(proto_rep_path: str, json_rep_path: str):
    with open(proto_rep_path, "rb") as f:
        parsed_binary = trace_collector.ExportTraceServiceRequest().parse(f.read())
    with open(json_rep_path, "r") as f:
        expected_json = json.load(f)

    # To simplify debugging of tests
    # with open(f"tmp_{json_rep_path}_bin.json".replace("/", "__"), "w") as f:
    #     f.write(
    #         json.dumps(
    #             json.loads(
    #                 otlpjson.OTLPJsonSpanCollection(expected_json).to_otlp_json()
    #             ),
    #             indent=2,
    #         )
    #     )
    # with open(f"tmp_{json_rep_path}_orig.json".replace("/", "__"), "w") as f:
    #     f.write(json.dumps(expected_json, indent=2))

    assert otlpjson.OTLPJsonSpanCollection(expected_json).to_otlp_json() == json.dumps(
        expected_json
    )

    # with open(f'tmp_{json_rep_path}_bin.json'.replace("/", "__"), 'w') as f:
    #     json.dump(to_otlp_jsonlike(parsed_binary), f, indent=2)
    # with open(f'tmp_{json_rep_path}_orig.json'.replace("/", "__"), 'w') as f:
    #     json.dump(expected_json, f, indent=2)

    assert (
        otlpjson.OTLPJsonSpanCollection(expected_json).to_otlp_protobuf()
        == parsed_binary
    )
