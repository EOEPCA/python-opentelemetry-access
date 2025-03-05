import python_opentelemetry_access.base as base
import python_opentelemetry_access.opensearch.ss4o as ss4o
# import python_opentelemetry_access.util as util

# from dataclasses import dataclass
from collections.abc import AsyncIterable
from typing import List, Optional, Tuple, override
from datetime import datetime

import python_opentelemetry_access.proxy as proxy

from opensearchpy import AsyncOpenSearch


class OpenSearchSS40Proxy(proxy.Proxy):
    def __init__(self, client: AsyncOpenSearch, page_size: int = 100):
        self.client = client
        self.index_name = "ss4o_traces-default-namespace"
        self.page_size = page_size

    @override
    async def query_spans_page(
        self,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        span_ids: Optional[List[Tuple[Optional[str], Optional[str]]]] = None,
        resource_attributes: Optional[dict[str, list[str]]] = None,
        scope_attributes: Optional[dict[str, list[str]]] = None,
        span_attributes: Optional[dict[str, list[str]]] = None,
        page_token: Optional[proxy.PageToken] = None,
    ) -> AsyncIterable[base.SpanCollection | proxy.PageToken]:
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
                filter.append({"match": {"traceId": trace_id}})

            if span_id is not None:
                filter.append({"match": {"spanId": span_id}})

        def attributes_to_filters(
            attributes: dict[str, list[str]], key_prefix: str
        ) -> list[object]:
            result: list[object] = []
            for key, values in attributes.items():
                match values:
                    case []:
                        pass
                    case [value]:
                        result.append({"term": {key_prefix + key + ".keyword": {"value": value}}})
                    case values:
                        result.append({"terms": {key_prefix + key + ".keyword": values}})
            return result

        if resource_attributes is not None:
            filter.extend(attributes_to_filters(resource_attributes, "resource."))

        if scope_attributes is not None:
            filter.extend(
                attributes_to_filters(scope_attributes, "instrumentationScope.")
            )
        
        if span_attributes is not None:
            filter.extend(attributes_to_filters(span_attributes, "attributes."))

        q = {
            "size": self.page_size,
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
            _ = datetime.fromisoformat(token)
            q["search_after"] = [token]

        results = await self.client.search(body=q, index=self.index_name)

        ## There should be a more clever way of doing this, but
        ## we cannot rely on results['hits']['total']['value'], since
        ## it does not take search_after into account
        if len(results["hits"]["hits"]) == self.page_size:
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
