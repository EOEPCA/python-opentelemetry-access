# Build image to compile all packages
FROM ghcr.io/astral-sh/uv:python3.12-bookworm as build

WORKDIR /app

COPY . /app/

RUN uv sync --python-preference only-system --no-dev --frozen --compile-bytecode --no-editable

FROM python:3.12-slim-bookworm as runner

WORKDIR /app

COPY --from=build /app/.venv /app/.venv

EXPOSE 8080

ENTRYPOINT ["/app/.venv/bin/python"]

#CMD ["-m", "python_opentelemetry_access", "proxy", "--host", "0.0.0.0", "--port", "8080", "opensearch-ss4o", "--oshost", ""${OPENSEARCH_HOST}"", "--osport", ""${OPENSEARCH_PORT}"", "--ca_certs", "/certs/ca.crt", "--client_cert", "/certs/tls.crt", "--client_key", "/certs/tls.key"]
CMD ["-m", "python_opentelemetry_access", "proxy","--host", "0.0.0.0", "--port", "8080", "opensearch-ss4o", "--oshost", "opensearch-cluster-master-headless", "--osport", "9200", "--ca_certs", "/certs/ca.crt", "--client_cert", "/certs/tls.crt", "--client_key", "/certs/tls.key"]
