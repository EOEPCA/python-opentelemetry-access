import opensearchpy
from collections.abc import AsyncIterable
from typing import List, Never, Optional, Tuple, override, assert_never, Any
from datetime import datetime
import os

from opensearchpy import AsyncOpenSearch

from python_opentelemetry_access import util
from python_opentelemetry_access.api_utils.exceptions import APIException
from python_opentelemetry_access.api_utils.json_api_types import Error

import python_opentelemetry_access.base as base
import python_opentelemetry_access.opensearch.ss4o as ss4o
from python_opentelemetry_access.util import InvalidPageTokenException
import python_opentelemetry_access.proxy as proxy

from python_opentelemetry_access.telemetry_hooks import Hook, run_hook_async


def raise_error_from_transport_error(
    e: opensearchpy.TransportError, default_status: int
) -> Never:
    match e.info:
        case Exception():
            detail = repr(e.info)
        case {"error": {"root_cause": [{"reason": reason}]}}:
            detail = reason
        case _:
            detail = e.error

    raise APIException(
        Error(
            status=str(default_status)
            if e.status_code == "N/A"
            else str(e.status_code),
            code=e.error,
            title=e.__class__.__name__,
            detail=detail,
        )
    )


GET_OPENSEARCH_CONFIG_HOOK_NAME = (
    os.environ.get("RH_TELEMETRY_GET_OPENSEARCH_CONFIG_HOOK_NAME")
    or "get_opensearch_config"
)


class OpenSearchSS40Proxy(proxy.Proxy):
    def __init__(
        self, hooks: dict[str, Hook], default_page_size: int, max_page_size: int
    ) -> None:
        self.hooks = hooks
        self.index_name = "ss4o_traces-default-namespace"
        self.default_page_size = default_page_size
        self.max_page_size = max_page_size

    @override
    async def query_spans_page(
        self,
        auth_info: Any,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]] = None,
        resource_attributes: Optional[util.AttributesFilter] = None,
        scope_attributes: Optional[util.AttributesFilter] = None,
        span_attributes: Optional[util.AttributesFilter] = None,
        span_name: Optional[str] = None,
        page_size: Optional[int] | None = None,
        page_token: Optional[proxy.PageToken] = None,
    ) -> AsyncIterable[base.SpanCollection | proxy.PageToken]:
        if page_size is None:
            page_size = self.default_page_size
        if page_size < 1:
            page_size = 1
        if page_size > self.max_page_size:
            page_size = self.max_page_size

        filter: list[object] = []
        if from_time is not None:
            filter.append({"range": {"startTime": {"gte": from_time.isoformat()}}})

        if to_time is not None:
            filter.append({"range": {"endTime": {"lte": to_time.isoformat()}}})

        if span_ids is not None:
            if len(span_ids) != 1:
                raise RuntimeError(
                    "Currently only supports selecting precisely one trace/span"
                )
            (trace_id, span_id) = span_ids[0]

            if trace_id is not None:
                filter.append({"term": {"traceId": trace_id}})

            if span_id is not None:
                filter.append({"term": {"spanId": span_id}})

        def attribbute_to_filter(
            key_prefix: str, key: str, value: str | int | float | bool
        ) -> dict[str, object]:
            key_suffix = ".keyword" if isinstance(value, str) else ""
            return {"term": {key_prefix + key + key_suffix: {"value": value}}}

        def attributes_to_filters(
            attributes: util.AttributesFilter, key_prefix: str
        ) -> list[object]:
            result: list[object] = []
            for key, values in attributes.items():
                match values:
                    case None:
                        result.append({"exists": {"field": key_prefix + key}})
                    case []:
                        pass
                    case [value]:
                        result.append(attribbute_to_filter(key_prefix, key, value))
                    case list(values):
                        result.append(
                            {
                                "bool": {
                                    # should acts as an OR here, as explained in
                                    # https://discuss.elastic.co/t/how-do-i-create-a-boolean-or-filter/282281
                                    "should": [
                                        attribbute_to_filter(key_prefix, key, value)
                                        for value in values
                                    ]
                                }
                            }
                        )
                    case unreachable:
                        assert_never(unreachable)
            return result

        if resource_attributes is not None:
            filter.extend(attributes_to_filters(resource_attributes, "resource."))

        if scope_attributes is not None:
            filter.extend(
                attributes_to_filters(scope_attributes, "instrumentationScope.")
            )

        if span_attributes is not None:
            filter.extend(attributes_to_filters(span_attributes, "attributes."))

        if span_name is not None:
            filter.append({"term": {"name.keyword": {"value": span_name}}})

        q: dict[str, Any] = {
            "size": page_size,
            "query": {"bool": {"filter": filter}},
            "sort": [
                {"startTime": {"order": "asc"}}
                # {"traceId": {"order": "asc"}},
                # {"spanId": {"order": "asc"}}
            ],
        }
        if page_token is not None:
            # q['search_after'] = page_token.token.decode('ascii').split("__")
            ## Validate
            token = page_token.token.decode("ascii")
            try:
                _ = datetime.fromisoformat(token)
            except ValueError:
                raise InvalidPageTokenException.create()
            q["search_after"] = [token]

        if GET_OPENSEARCH_CONFIG_HOOK_NAME not in self.hooks:
            raise ValueError(
                f"Must set hook {GET_OPENSEARCH_CONFIG_HOOK_NAME} ($GET_OPENSEARCH_CONFIG_HOOK_NAME) when using the OpenSearch backend"
            )

        client_config = await run_hook_async(
            self.hooks[GET_OPENSEARCH_CONFIG_HOOK_NAME], auth_info
        )

        try:
            client = AsyncOpenSearch(**client_config)
            results = await client.search(
                body=q,
                index=self.index_name,
                headers=client_config.get("extra_headers"),
            )
        # Don't want to turn all connection exceptions to something visible to the end user
        # to not expose implementation details and things that might be secret
        except opensearchpy.AuthenticationException as e:
            raise_error_from_transport_error(e, 401)
        except opensearchpy.AuthorizationException as e:
            raise_error_from_transport_error(e, 403)
        except opensearchpy.ConnectionTimeout as e:
            raise_error_from_transport_error(e, 500)
        except opensearchpy.NotFoundError as e:
            # At least for now a non-existent index is considered empty
            if e.error == "index_not_found_exception":
                # TODO: do we to put these hardcoded things in here just to keep the format consistent?
                results = {
                    "took": 10,
                    "timed_out": False,
                    "_shards": {"total": 0, "successful": 0, "skipped": 0, "failed": 0},
                    "hits": {
                        "total": {"value": 0, "relation": "eq"},
                        "max_score": None,
                        "hits": [],
                    },
                }
            else:
                raise_error_from_transport_error(e, 404)
        finally:
            await client.close()

        ## There should be a more clever way of doing this, but
        ## we cannot rely on results['hits']['total']['value'], since
        ## it does not take search_after into account
        if len(results["hits"]["hits"]) == page_size:
            # next_page_token = \
            #    (last_hit["_source"]["traceId"]+'__'+last_hit["_source"]["spanId"]).encode('ascii')
            next_page_token = results["hits"]["hits"][-1]["_source"][
                "startTime"
            ].encode("ascii")
        else:
            next_page_token = None

        yield ss4o.SS4OSpanCollection(results)

        if next_page_token is not None:
            yield proxy.PageToken(next_page_token)

    @override
    async def aclose(self) -> None:
        pass
