# Build image to compile all packages
FROM python:3.11 as build

COPY . /project
WORKDIR /project
RUN pip install uv && uv sync --no-editable

FROM python:3.11

COPY --from=build /project/.venv /venv

#CMD ["/venv/bin/python", "-m", "python_opentelemetry_access", "proxy", "--host", "0.0.0.0", "--port", "8080", "opensearch-ss4o", "--oshost", ""${OPENSEARCH_HOST}"", "--osport", ""${OPENSEARCH_PORT}"", "--ca_certs", "/certs/ca.crt", "--client_cert", "/certs/tls.crt", "--client_key", "/certs/tls.key"]
CMD ["/venv/bin/python", "-m", "python_opentelemetry_access", "proxy","--host", "0.0.0.0", "--port", "8080", "opensearch-ss4o", "--oshost", "opensearch-cluster-master-headless", "--osport", "9200", "--ca_certs", "/certs/ca.crt", "--client_cert", "/certs/tls.crt", "--client_key", "/certs/tls.key"]