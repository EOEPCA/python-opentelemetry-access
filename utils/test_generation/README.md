# Convert test cases to OpenSearch ss4o

To convert a test-case into OpenSearch ss4o format:

Start an OpenSearch cluster, e.g. using docker
```
docker run --rm -e OPENSEARCH_INITIAL_ADMIN_PASSWORD='V3ryS3cr3t!' --name opensearch -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" opensearchproject/opensearch:2.17.1
```

Setup OpenSearch collector to listen for OTLP-JSON files, e.g. with a config like:
```yaml
extensions:
  basicauth:
    client_auth: 
      username: admin
      password: "V3ryS3cr3t!"

receivers:
  otlpjsonfile:
    include:
      - "telemetry_dump/*.json"

exporters:
  debug:
    verbosity: detailed

  opensearch:
    http:
      endpoint: https://localhost:9200
      tls:
          insecure: false
          insecure_skip_verify: true
      auth:
        authenticator: basicauth

service:
  extensions: [basicauth]
  pipelines:
    traces:
      receivers: [otlpjsonfile]
      exporters: [opensearch, debug]
      processors: []
  telemetry:
    metrics:
      address: 0.0.0.0:8889
```

Copy an OTLP-JSON file into the directory set in the config (e.g. `telemetry_dump/` in the above example).

Fetch the results using the OpenSearch REST API
```
curl -s --insecure -u 'admin:V3ryS3cr3t!' https://localhost:9200/ss4o_traces-default-namespace/_search | jq '[ .hits.hits.[]._source ]'
```

Clear the index
```
curl -X DELETE -s --insecure -u 'admin:V3ryS3cr3t!' https://localhost:9200/ss4o_traces-default-namespace
```

Repeat...