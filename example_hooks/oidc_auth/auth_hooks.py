import python_opentelemetry_access.telemetry_hooks.utils as th

from eoepca_security import OIDCProxyScheme, Tokens
from typing import TypedDict
import os


class UserInfo(TypedDict):
    username: str | None
    access_token: str


def get_fastapi_security() -> OIDCProxyScheme:
    return OIDCProxyScheme(
        openIdConnectUrl=os.environ["OPEN_ID_CONNECT_URL"],
        audience=os.environ["OPEN_ID_CONNECT_AUDIENCE"],
        id_token_header="x-id-token",
        refresh_token_header="x-refresh-token",
        auth_token_header="Authorization",
        auth_token_in_authorization=True,
        auto_error=True,  ## Set False to allow unauthenticated access!
        scheme_name="OIDC behind auth proxy",
    )


def on_auth(tokens: Tokens | None) -> UserInfo:
    print("ON AUTH")

    if tokens is None or tokens["auth"] is None:
        raise th.APIException(
            th.Error(
                status="403",
                code="MissingTokens",
                title="Missing authentication token",
                detail="Potentially missing authenticating proxy",
            )
        )

    username_claim = (
        os.environ.get("RH_TELEMETRY_USERNAME_CLAIM") or "preferred_username"
    )

    return UserInfo(
        username=tokens["id"].decoded[username_claim]
        if tokens["id"] is not None and username_claim in tokens["id"].decoded
        else tokens["auth"].decoded["payload"].get(username_claim),
        access_token=tokens["auth"].raw,
    )


## For the OpenSearch proxy/backend


def get_opensearch_config(userinfo: UserInfo) -> th.OpensearchConfig:
    return th.OpensearchConfig(
        ## Host to connect to
        # hosts=[{"host": "localhost", "port": 9200}],
        hosts=[{"host": "opensearch-cluster-master-headless", "port": 9200}],
        use_ssl=True,
        ## For unverified tls
        # verify_certs=False,
        # ssl_show_warn=False,
        ## For verified tls
        verify_certs=True,
        ssl_show_warn=True,
        ca_certs="/certs/ca.crt",
        ## For mTLS auth
        # client_cert = "/certs/tls.crt"
        # client_key = "/certs/tls.key"
        # Authenticate by forwarding user token
        extra_headers={"Authorization": f"Bearer {userinfo['access_token']}"},
    )
