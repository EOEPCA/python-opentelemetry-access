from python_opentelemetry_access.api_utils.exceptions import APIException
from python_opentelemetry_access.api_utils.json_api_types import Error

from typing import TypedDict, Any, NotRequired


class OpensearchConfig(TypedDict):
    hosts: list[str] | list[dict[str, Any]]  # = []
    retry_on_timeout: NotRequired[bool]  # = False

    use_ssl: NotRequired[bool]  # = True
    verify_certs: NotRequired[bool]  # = (True,)
    ssl_assert_hostname: NotRequired[bool]  # =False,
    ssl_show_warn: NotRequired[bool]  # =False

    ca_certs: NotRequired[str]
    client_cert: NotRequired[str]
    client_key: NotRequired[str]

    extra_headers: NotRequired[dict[str, str]]


__all__ = ["APIException", "Error", "OpensearchConfig"]
