import asyncio

from .base import DEFAULT_DRY_RUN, set_logging
from .queue import download_cases_add_results
# from .sheet import main


def run_sync(dry_run: bool = DEFAULT_DRY_RUN):
  set_logging()

  asyncio.run(
    download_cases_add_results(dry_run),
    debug=True,
  )


if __name__ == "__main__":
  run_sync()
