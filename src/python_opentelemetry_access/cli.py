import asyncio
from enum import StrEnum
from logging import INFO, WARNING, getLogger
from importlib.metadata import version
from os import environ
from pathlib import Path
from sys import stdin, stdout
from typing import Optional
from typing_extensions import Annotated

from opensearchpy import AsyncOpenSearch
from typer import Argument, Context, Exit, Option, Typer
import uvicorn

import python_opentelemetry_access.api as api
import python_opentelemetry_access.opensearch.ss4o as ss4o
import python_opentelemetry_access.otlpjson as otlpjson
import python_opentelemetry_access.otlpproto as otlpproto
import python_opentelemetry_access.proxy as proxy_mod
import python_opentelemetry_access.proxy.opensearch.ss4o as ss4o_proxy

cli = Typer(
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]}
)
proxy = Typer(no_args_is_help=True)
cli.add_typer(proxy, name="proxy")

logger = getLogger("python-opentelemetry-access")


def version_callback(value: bool):
    if value:
        print(
            "python-opentelemetry-access version " +
            version("python-opentelemetry-access")
        )
        raise Exit()


@cli.callback()
def cli_callback(
    ctx: Context,
    verbose: Annotated[
        bool,
        Option(
            "--verbose/--no-verbose", "-v",
            show_default=False,
        )
    ] = False,
    version: bool = Option(
        None,
        "--version",
        help="Print version information and exit.",
        callback=version_callback,
    ),
) -> None:
    """
    python-opentelemetry-access command line interface
    """
    ctx.obj = {"verbose": verbose}
    logger.setLevel(INFO if verbose else WARNING)


def dump_otlp_json(x, f):
    for chunk in x.to_otlp_json_str_iter():
        f.write(chunk)


def dump_otlp_proto(x, f):
    f.write(bytes(x.to_otlp_protobuf()))


IN_FORMATS = {
    "ss4o_bare": (False, ss4o.load_bare),
    "ss4o": (False, ss4o.load),
    "otlp-json": (False, otlpjson.load),
    "otlp-proto": (True, otlpproto.load),
}


OUT_FORMATS = {
    "otlp-json": (False, dump_otlp_json),
    "otlp-proto": (True, dump_otlp_proto),
}


@cli.command()
def list_formats() -> None:
    for f in sorted(set(IN_FORMATS.keys()).union(OUT_FORMATS.keys())):
        match(f in IN_FORMATS, f in OUT_FORMATS):
            case(True, True):
                cap = "in and out"
            case(True, False):
                cap = "only in"
            case(False, True):
                cap = "only out"
            case _:
                pass
        print(f"- {f} ({cap})")


InFormat = StrEnum("InFormat", tuple(IN_FORMATS.keys()))
OutFormat = StrEnum("OutFormat", tuple(OUT_FORMATS.keys()))


@cli.command()
def convert(
    infile: Annotated[Path, Argument(
        exists=True,
        resolve_path=True,
    )],
    outfile: Annotated[Path, Argument(
        exists=False,
        resolve_path=True,
    )],
    from_: Annotated[InFormat, Option(
        "-f", "--from",
        help="Input format",
        show_default=False,
    )],
    to: Annotated[OutFormat, Option(
        "-t", "--to",
        help="Output format",
        show_default=False,
    )],
) -> None:
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
    api.settings.proxy = proxy

    uvicorn.run(
        api.cli,
        host=ctx.obj.get("host") or "127.0.0.1",
        port=ctx.obj.get("port") or 12345,
        reload=False,
        log_level="debug",
        workers=1,
        root_path=ctx.obj.get("root_path") or ""
    )


@proxy.callback()
def proxy_callback(
    ctx: Context,
    host: Annotated[str, Option("--host")] = "127.0.0.1",
    port: Annotated[int, Option("--port")] = 12345,
    root_path: Annotated[Optional[str], Option("--root-path")] = None,
) -> None:
    ctx.obj["host"] = host
    ctx.obj["port"] = port
    ctx.obj["root_path"] = root_path or environ.get("FAST_API_ROOT_PATH")


@proxy.command()
def mock(
    ctx: Context,
    file: Annotated[Path, Option(
        "--file",
        exists=True,
        resolve_path=True,
    )],
) -> None:
    with open(file, "r") as f:
        this_proxy = proxy_mod.MockProxy(otlpjson.load(f))

    run_proxy(ctx, this_proxy)


@proxy.command()
def opensearch_ss4o(
    ctx: Context,
    oshost: Annotated[str, Option("--oshost")] = "127.0.0.1",
    osport: Annotated[int, Option("--osport")] = 9200,
    osuser: Annotated[Optional[str], Option("--osuser")] = None,
    ospass: Annotated[Optional[str], Option("--ospass")] = None,
    ca_certs: Annotated[Path, Option(
        "--ca_certs",
        exists=True,
        allow_dash=False,
    )] = None,
    client_cert: Annotated[Path, Option(
        "--client_cert",
        exists=True,
        allow_dash=False,
    )] = None,
    client_key: Annotated[Path, Option(
        "--client_key",
        exists=True,
        allow_dash=False,
    )] = None,
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
