# type: ignore
import python_opentelemetry_access.opensearch.ss4o as ss4o
import python_opentelemetry_access.otlpjson as otlpjson
import python_opentelemetry_access.otlpproto as otlpproto

import python_opentelemetry_access.proxy as proxy_mod
import python_opentelemetry_access.proxy.opensearch.ss4o as ss4o_proxy
import python_opentelemetry_access.api as api
from python_opentelemetry_access.api_utils.api_utils import get_env_var_or_throw

import uvicorn
from opensearchpy import AsyncOpenSearch

from typing import Optional
import asyncio
import logging
from os import environ
from pathlib import Path
from sys import stdin, stdout

import click

logger = logging.getLogger("python-opentelemetry-access")


CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option("--verbose/--no-verbose", "-v", default=False)
# @click.version_option(version=0.1.0, prog_name="python-opentelemetry-access")
@click.pass_context
def cli(ctx, verbose: bool) -> None:
    """
    python-opentelemetry-access command line interface
    """
    ctx.obj = {"verbose": verbose}
    logger.setLevel(logging.INFO if verbose else logging.WARNING)


IN_FORMATS = {
    "ss4o_bare": (False, ss4o.load_bare),
    "ss4o": (False, ss4o.load),
    "otlp-json": (False, otlpjson.load),
    "otlp-proto": (True, otlpproto.load),
}


def dump_otlp_json(x, f):
    for chunk in x.to_otlp_json_str_iter():
        f.write(chunk)


def dump_otlp_proto(x, f):
    f.write(bytes(x.to_otlp_protobuf()))


OUT_FORMATS = {
    "otlp-json": (False, dump_otlp_json),
    "otlp-proto": (True, dump_otlp_proto),
}


@cli.command()
def list_formats() -> None:
    for f in sorted(set(IN_FORMATS.keys()).union(OUT_FORMATS.keys())):
        match (f in IN_FORMATS, f in OUT_FORMATS):
            case (True, True):
                cap = "in and out"
            case (True, False):
                cap = "only in"
            case (False, True):
                cap = "only out"
            case _:
                pass
        print(f"- {f} ({cap})")


@cli.command()
@click.argument(
    "infile", nargs=1, type=click.Path(exists=True, path_type=Path, allow_dash=True)
)
@click.argument(
    "outfile", nargs=1, type=click.Path(exists=False, path_type=Path, allow_dash=True)
)
@click.option("--from", "-f", "from_", type=click.Choice(list(IN_FORMATS.keys())))
@click.option("--to", "-t", type=click.Choice(list(OUT_FORMATS.keys())))
def convert(infile: Path, outfile: Path, from_: str, to: str) -> None:
    """
    Converts from one representation to another
    """

    is_binary_in, reader = IN_FORMATS[from_]
    if infile.name == "-":
        if is_binary_in:
            rep = reader(stdin.buffer)
        else:
            rep = reader(stdin)
    else:
        if is_binary_in:
            with open(infile, "rb") as f:
                rep = reader(f)
        else:
            with open(infile, "r") as f:
                rep = reader(f)

    is_binary_out, writer = OUT_FORMATS[to]
    if outfile.name == "-":
        if is_binary_out:
            writer(rep, stdout.buffer)
        else:
            writer(rep, stdout)
        stdout.flush()
    else:
        if is_binary_out:
            with open(outfile, "wb") as f:
                writer(rep, f)
        else:
            with open(outfile, "w") as f:
                writer(rep, f)


def run_proxy(ctx, proxy):
    api.settings._proxy = proxy
    api.settings._base_url = get_env_var_or_throw("RH_TELEMETRY_API_BASE_URL")
    api.settings._open_id_connect_url = get_env_var_or_throw(
        "RH_TELEMETRY_API_OPEN_ID_CONNECT_URL"
    )
    api.settings._open_id_connect_audience = (
        environ.get("RH_TELEMETRY_API_OPEN_ID_CONNECT_AUDIENCE") or "account"
    )

    uvicorn.run(
        api.app,
        host=ctx.obj.get("host") or "127.0.0.1",
        port=ctx.obj.get("port") or 12345,
        reload=False,
        log_level="debug",
        workers=1,
        root_path=ctx.obj.get("root_path") or "",
    )


@cli.group(context_settings=CONTEXT_SETTINGS)
@click.option("--host", default="127.0.0.1")
@click.option("--port", default=12345)
@click.option("--root-path", default=None)
@click.pass_context
def proxy(ctx, host: str, port: str, root_path: str) -> None:
    ctx.obj["host"] = host
    ctx.obj["port"] = port
    ctx.obj["root_path"] = root_path or environ.get("FAST_API_ROOT_PATH")


@proxy.command()
@click.option(
    "--file",
    required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
)
@click.pass_context
def mock(ctx, file) -> None:
    with open(file, "r") as f:
        this_proxy = proxy_mod.MockProxy(otlpjson.load(f))

    run_proxy(ctx, this_proxy)


@proxy.command()
@click.option("--oshost", default="127.0.0.1")
@click.option("--osport", default=9200)
@click.option("--osuser", default=None)
@click.option("--ospass", default=None)
@click.option("--ca_certs", default=None)
@click.option("--client_cert", default=None)
@click.option("--client_key", default=None)
@click.pass_context
def opensearch_ss4o(
    ctx,
    oshost: str,
    osport: str,
    osuser: Optional[str],
    ospass: Optional[str],
    ca_certs: click.Path(exists=True, path_type=Path, allow_dash=False),
    client_cert: click.Path(exists=True, path_type=Path, allow_dash=False),
    client_key: click.Path(exists=True, path_type=Path, allow_dash=False),
) -> None:
    opensearch_params = {"verify_certs": False, "ssl_show_warn": False}

    if osuser is not None and ospass is not None:
        opensearch_params["http_auth"] = (osuser, ospass)
    if ca_certs is not None:
        opensearch_params["ca_certs"] = str(ca_certs)
        opensearch_params.update({"verify_certs": True, "ssl_show_warn": True})
    if client_cert is not None:
        opensearch_params["client_cert"] = str(client_cert)
        opensearch_params.update({"verify_certs": True, "ssl_show_warn": True})
    if client_key is not None:
        opensearch_params["client_key"] = str(client_key)
        opensearch_params.update({"verify_certs": True, "ssl_show_warn": True})

    client = AsyncOpenSearch(
        hosts=[{"host": oshost, "port": osport}], use_ssl=True, **opensearch_params
    )

    try:
        proxy = ss4o_proxy.OpenSearchSS40Proxy(client)
        run_proxy(ctx, proxy)
    finally:
        asyncio.run(client.close())
