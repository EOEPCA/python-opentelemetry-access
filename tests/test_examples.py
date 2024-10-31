import json

from typing import no_type_check

# import opentelemetry_betterproto.opentelemetry.proto.common.v1 as common
# import opentelemetry_betterproto.opentelemetry.proto.collector.trace.v1 as trace_collector

import python_opentelemetry_access.util as util
import python_opentelemetry_access.otlpjson as otlpjson
import python_opentelemetry_access.otlpproto as otlpproto
import python_opentelemetry_access.opensearch_ss4o as opensearch_ss4o

from pytest import mark


@no_type_check
@mark.parametrize(
    "proto_rep_path, json_rep_path, ss4o_rep_path",
    [
        (
            "tests/examples/ex1.binpb",
            "tests/examples/ex1.json",
            "tests/examples/ex1_ss4o_bare.json",
        ),
        (
            "tests/examples/ex2.binpb",
            "tests/examples/ex2.json",
            "tests/examples/ex2_ss4o_bare.json",
        ),
    ],
)
def test_example_trace(proto_rep_path: str, json_rep_path: str, ss4o_rep_path: str):
    with open(proto_rep_path, "rb") as f1:
        parsed_binary = otlpproto.load(f1)
    with open(json_rep_path, "r") as f2:
        expected_json = otlpjson.load(f2)
    with open(ss4o_rep_path, "r") as f3:
        ss4o_search_results = opensearch_ss4o.load_bare(f3)

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

    # with open(f"tmp_{json_rep_path}_ss4o.json".replace("/", "__"), "w") as f:
    #     json.dump(
    #         util.force_jsonlike_iter(
    #             opensearch_ss4o.SS4OSpanCollection(
    #                 ss4o_search_results
    #             ).to_otlp_json_iter()
    #         ),
    #         f,
    #         indent=2,
    #     )
    # with open(f"tmp_{json_rep_path}_orig.json".replace("/", "__"), "w") as f:
    #     json.dump(expected_json, f, indent=2)

    assert expected_json.to_otlp_json() == json.dumps(expected_json.jobj)
    assert parsed_binary.to_otlp_json() == expected_json.to_otlp_json()

    assert expected_json.to_otlp_protobuf() == parsed_binary._proto

    assert parsed_binary.to_otlp_protobuf() == parsed_binary._proto

    ## TODO: Currently (somehow) resource does not get set in OpenSearch
    ##       This is an issue with the setup, not with this library.
    ss4o_scope_spans = [
        util.force_jsonlike_dict_iter(sss.to_otlp_json_iter())
        for rss in ss4o_search_results.otlp_resource_spans
        for sss in rss.otlp_scope_spans
    ]

    otlpjson_scope_spans = [
        util.force_jsonlike_dict_iter(sss.to_otlp_json_iter())
        for rss in expected_json.otlp_resource_spans
        for sss in rss.otlp_scope_spans
    ]

    def sort_otlp_attributes(kvs):
        return sorted(kvs, key=lambda x: x["key"])

    for sss in ss4o_scope_spans:
        for scope in sss["spans"]:
            scope["attributes"] = sort_otlp_attributes(
                [
                    attribute
                    for attribute in scope["attributes"]
                    if attribute["key"]
                    != "data_stream"  ## Additional attribute set by OpenSearch
                ]
            )

            if "events" in scope:
                for event in scope["events"]:
                    event["attributes"] = sort_otlp_attributes(event["attributes"])

    for sss in otlpjson_scope_spans:
        for scope in sss["spans"]:
            if "flags" in scope:
                del scope["flags"]  ## OpenSearch does not store flags

            scope["attributes"] = sort_otlp_attributes(scope["attributes"])

            if "events" in scope:
                for event in scope["events"]:
                    event["attributes"] = sort_otlp_attributes(event["attributes"])

    # with open(f"tmp_{json_rep_path}_ss4o.json".replace("/", "__"), "w") as f:
    #     json.dump(
    #         ss4o_scope_spans,
    #         f,
    #         sort_keys=True,
    #         indent=2,
    #     )
    # with open(f"tmp_{json_rep_path}_orig.json".replace("/", "__"), "w") as f:
    #     json.dump(otlpjson_scope_spans, f, sort_keys=True, indent=2)

    assert ss4o_scope_spans == otlpjson_scope_spans
