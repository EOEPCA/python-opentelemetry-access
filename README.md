# Higher level library for processing OpenTelemetry data using Python

**WARNING**: Name is subject to change

The library helps navigate OpenTelemetry data and convert from/to OTLP JSON and OTLP protobuf 
representations. Currently only Trace data is supported.

## Basic usage

This package provides abstract interfaces for traversing OpenTelemetry data in a way that is
aligned with the representation used in OTLP. The basic workflow is

1. Load the submodule appropriate for your concrete representation, e.g.

   ```python
   import python_opentelemetry_access.opensearch.ss4o as ss4o
   ```

   for the representation used in search results from the OpenSearch REST API when
   data is stored according to the ss4o schema (as used by the OpenSearch exporter for the
   OpenTelemetry collector).

2. Use one of the module's `load*` functions to load your data, the typical pattern is `load`
   for loading from a file-like object, `loads` for loading from string/bytes, `loado` for loading
   from an appropriate Python object. In the case of `opensearch.ss4o` there are two variants, `load*`
   and `load*_bare`. The latter accepts a list of actual stored documents
   ```python
   with open('tests/examples/ex1_ss4o_bare.json', 'r') as f:
       all_spans = ss4o.load_bare(f)
   ```

3. Traverse the actual spans in the span collection
   ```python
   for resource_spans in all_spans.otlp_resource_spans:
       print(f"Resource: {resource_spans.otlp_resource.otlp_attributes}")
       for scope_spans in resource_spans.otlp_scope_spans:
           print(f"  Scope: {scope_spans.otlp_scope.otlp_name}")
           for span in scope_spans.otlp_spans:
               print(f"    Span: {span.otlp_name}")
   ```

   or convert it to OTLP-JSON/OTLP-Protobuf
   ```python
   print(all_spans.to_otlp_json())

   for chunk in all_spans.to_otlp_json_str_iter():
       print(chunk, end='')
    
   print(bytes(all_spans.to_otlp_protobuf()))
   ```

To fetch data, use the `python_opentelemetry_access.proxy` module (and its submodules).

TODO: add example

## CLI usage

The CLI can be used to convert between different formats. To list available formats use
```
$ python -m python_opentelemetry_access list-formats
- otlp-json (in and out)
- otlp-proto (in and out)
- ss4o (only in)
- ss4o_bare (only in)
```

To convert data use `convert`
```
$ uv run -m python_opentelemetry_access convert -f ss4o_bare -t otlp-json tests/examples/ex1_ss4o_bare.json out.json
$ cat out.json
{"resourceSpans": [...] }
```
Both the in-file and the out-file can be `-` in order to read from stdin/write to stdout.

## Running a server

The library includes a FastAPI endpoint that (effectively) exposes the `python_opentelemetry_access.proxy` module (and its submodules) as a REST-style API.

TODO: Describe for general backend

Currently it supports the `MockProxy` to expose a single otlp-json file
```
$ python -m python_opentelemetry_access proxy --host 0.0.0.0 --port 12345 mock --file tests/examples/ex2.json
```
or telemetry stored in OpenSearch using the ss4o schema
```
$ python -m python_opentelemetry_access proxy --host 0.0.0.0 --port 12345 opensearch-ss4o --oshost=... --osport=... --osuser=... --ospass=...
```

For example:
```
$ curl localhost:12345/v1/spans/697777f078628bc35093f4f376dfa62d/7f2aedeb88337ec1
[{"resource_spans": [...]}]
```
