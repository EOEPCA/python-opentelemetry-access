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

    assert expected_json.to_reified().to_otlp_json() == expected_json.to_otlp_json()
    assert parsed_binary.to_reified().to_otlp_json() == parsed_binary.to_otlp_json()
    assert (
        ss4o_search_results.to_reified().to_otlp_json()
        == ss4o_search_results.to_otlp_json()
    )

    assert expected_json.to_reified() == parsed_binary.to_reified()

    assert expected_json.to_otlp_json() == json.dumps(expected_json.jobj)
    assert parsed_binary.to_otlp_json() == expected_json.to_otlp_json()

    assert expected_json.to_otlp_protobuf() == parsed_binary._proto

    assert parsed_binary.to_otlp_protobuf() == parsed_binary._proto

    ss4o_search_results_ = util.force_jsonlike_dict_iter(
        ss4o_search_results.to_otlp_json_iter()
    )
    expected_json_ = util.force_jsonlike_dict_iter(expected_json.to_otlp_json_iter())

    def sort_otlp_attributes(kvs):
        return sorted(kvs, key=lambda x: x["key"])

    for rss in ss4o_search_results_["resourceSpans"]:
        rss["resource"]["attributes"] = sort_otlp_attributes(
            util.to_kv_list(
                util.normalise_attributes_shallow(
                    util.from_kv_list(rss["resource"]["attributes"])
                )
            )
        )
        for sss in rss["scopeSpans"]:
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

    for rss in expected_json_["resourceSpans"]:
        rss["resource"]["attributes"] = sort_otlp_attributes(
            util.to_kv_list(
                util.normalise_attributes_shallow(
                    util.from_kv_list(rss["resource"]["attributes"])
                )
            )
        )
        for sss in rss["scopeSpans"]:
            for scope in sss["spans"]:
                if "flags" in scope:
                    del scope["flags"]  ## OpenSearch does not store flags

                scope["attributes"] = sort_otlp_attributes(scope["attributes"])

                if "events" in scope:
                    for event in scope["events"]:
                        event["attributes"] = sort_otlp_attributes(event["attributes"])

    assert ss4o_search_results_ == expected_json_
