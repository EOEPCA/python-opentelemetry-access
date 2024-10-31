# Higher level library for processing OpenTelemetry data using Python

**WARNING**: Name is subject to change

The library helps navigate OpenTelemetry data and convert from/to OTLP JSON and OTLP protobuf 
representations. Currently only Trace data is supported.

## Basic usage

This package provides abstract interfaces for traversing OpenTelemetry data in a way that is
aligned with the representation used in OTLP. The basic workflow is

1. Load the submodule appropriate for your concrete representation, e.g.

   ```python
   import python_opentelemetry_access.opensearch_ss4o as ss4o
   ```

   for the representation used in search results from the OpenSearch REST API when
   data is stored according to the ss4o schema (as used by the OpenSearch exporter for the
   OpenTelemetry collector).

2. Use one of the module's `load*` functions to load your data, the typical pattern is `load`
   for loading from a file-like object, `loads` for loading from string/bytes, `loado` for loading
   from an appropriate Python object. In the case of `opensearch_ss4o` there are two variants, `load*`
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