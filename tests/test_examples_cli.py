from pathlib import Path
from importlib.metadata import version
from typing import no_type_check

from typer.testing import CliRunner

import python_opentelemetry_access.cli as cli


from pytest import mark


def test_version():
    result = CliRunner().invoke(cli.cli, ["--version"])
    assert result.exit_code == 0
    assert (
        "python-opentelemetry-access version " +
        version("python-opentelemetry-access")
    ) in result.stdout


def test_list_formats():
    result = CliRunner().invoke(cli.cli, ["list-formats"])
    assert result.exit_code == 0
    assert len(result.output.splitlines()) > 2


@no_type_check
@mark.parametrize(
    "proto_rep_path, json_rep_path, ss4o_rep_path",
    [
        (
            "tests/examples/ex1.binpb",
            "tests/examples/ex1.json",
            "tests/examples/ex1_ss4o_bare.json",
        ),
        (
            "tests/examples/ex2.binpb",
            "tests/examples/ex2.json",
            "tests/examples/ex2_ss4o_bare.json",
        ),
    ],
)
@mark.parametrize("to", [to for to in cli.OutFormat])
@mark.parametrize("outfile", [True, False])
def test_example_trace(
    tmp_path: Path,
    proto_rep_path: str,
    json_rep_path: str,
    ss4o_rep_path: str,
    to: str,
    outfile: bool,
):
    tmp_file: str = str(tmp_path / "tmp") if outfile else "-"

    result = CliRunner().invoke(
        cli.cli, [
            "convert",
            "--from=otlp-json",
            f"--to={to}",
            json_rep_path,
            tmp_file,
        ]
    )
    assert result.exit_code == 0

    result = CliRunner().invoke(
        cli.cli, [
            "convert",
            "--from=otlp-proto",
            f"--to={to}",
            proto_rep_path,
            tmp_file,
        ],
    )
    assert result.exit_code == 0

    result = CliRunner().invoke(
        cli.cli, [
            "convert",
            "--from=ss4o_bare",
            f"--to={to}",
            ss4o_rep_path,
            tmp_file,
        ]
    )
    assert result.exit_code == 0
