# Test data

This describes how the test data was generated.

## *.json

`ex1.json`, `ex2.json` etc. (but not ss4o stuff) are what OTEL collector writes with the json file exporter, reformatted and sligtly modified by hand. Collector config for that is in `utils/json_collector_config.json`. The data fed into the collector is from a **single** locally running instrumented healthcheck run. Multiple healthcheck runs would produce multiple jsons in the same file, which is means an invalid json file.

## *.binpb

`ex1.binpb`, `ex2.binpb` etc. are what OTEL collector writes with the binary (protobuf) file exporter with the first **4 bytes** removed. Collector config for that is in `utils/binary_collector_config.json`. The data fed into the collector is a single OTLP json file. E.g. to feed, `ex1.json` file to the collector, run

```bash
curl -H "Content-Type: application/json" -d @tests/examples/ex1.json http://localhost:4318/v1/traces
```

## *_ss4o_bare.json

`ex1_ss4o_bare.json1`, `ex2_ss4o_bare.json` are how Opensearch stores `ex1.json`, `ex2.json`, etc.
