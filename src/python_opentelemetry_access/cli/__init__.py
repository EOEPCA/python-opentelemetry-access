# type: ignore
import python_opentelemetry_access.opensearch_ss4o as ss4o
import python_opentelemetry_access.otlpjson as otlpjson
import python_opentelemetry_access.otlpproto as otlpproto

import python_opentelemetry_access.proxy as proxy
import python_opentelemetry_access.proxy.api as api

import uvicorn

import logging
from pathlib import Path
from sys import stdin, stdout

import click

logger = logging.getLogger("python-opentelemetry-access")


CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option("--verbose/--no-verbose", "-v", default=False)
# @click.version_option(version=0.1.0, prog_name="python-opentelemetry-access")
def cli(verbose: bool) -> None:
    """
    python-opentelemetry-access command line interface
    """
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


@cli.command()
@click.option("--host", default="0.0.0.0")
@click.option("--port", default=12345)
@click.option(
    "--file",
    required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
)
def run_proxy(host, port, file) -> None:
    with open(file, "r") as f:
        api.settings.proxy = proxy.MockProxy(otlpjson.load(f))
    uvicorn.run(
        api.app, host=host, port=port, reload=False, log_level="debug", workers=1
    )
