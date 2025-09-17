import logging

from python_opentelemetry_access.cli import cli

if __name__ == "__main__":
    logging.basicConfig(format="%(message)s", level=logging.WARNING)
    cli()
