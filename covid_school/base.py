#!/usr/bin/env python3
from __future__ import annotations
from typing import NamedTuple, Final, Awaitable, AsyncIterable, Iterable, \
  TypeVar
from re import compile, Pattern
from asyncio import as_completed, run, Task
import asyncio
import logging
import sys
import os

from aiohttp import ClientSession
from aiostream.aiter_utils import aitercontext
from limiter import Limiter
import logging

from .data import (
  Headers, SCHOOL_URLS, HEADERS
)


RELAX_ENV: Final[str] = 'OAUTHLIB_RELAX_TOKEN_SCOPE'
ENABLED: Final[str] = '1'

CASES_RE: Final[Pattern] = \
  compile('Positive COVID-19 Cases Reported Since Previous School Day:\s+(\d+)')

REFRESH_RATE: Final[int] = 2
BURST_RATE: Final[int] = 4
DOWNLOAD_COST: Final[float] = 1.75

USER_FILE: Final[str] = 'token.json'

DEFAULT_DRY_RUN: Final[bool] = False
MIN_ARGS: Final[int] = 2


limit_downloads: Final[Limiter] = Limiter(
  rate=REFRESH_RATE,
  capacity=BURST_RATE,
  consume=DOWNLOAD_COST,
)


T = TypeVar('T')
U = TypeVar('U')

BackgroundTask = Task


class School(NamedTuple):
  name: str
  url: str

  async def get_cases(self, session: ClientSession | None = None) -> Result:
    content = await download(self.url, session)
    cases = get_cases(content)

    return Result(self, cases)


class Result(NamedTuple):
  school: School
  cases: int | None


def set_logging(level: int = logging.INFO):
  _, *args = sys.argv

  if args and (level := first(args)):
    level = int(level)

  logging.basicConfig(level=level)


def first(
  it: Iterable[T],
  default: U | None = None,
) -> T | U | None:
  it = iter(it)
  return next(it, default)


def background(coro: Awaitable[T]) -> BackgroundTask[T]:
  return asyncio.create_task(coro)


def should_dry_run(dry_run: bool = False) -> bool:
  _, *args = sys.argv

  if len(args) >= MIN_ARGS:
    _, dry_run, *_ = args

  return bool(int(dry_run))


def get_session(headers: Headers | None = None) -> ClientSession:
  if headers is None:
    headers: Headers = HEADERS.copy()

  return ClientSession(headers=headers)


async def download(url: str, session: ClientSession | None = None) -> str:
  if not session:
    session = get_session()

  async with (
    session,
    limit_downloads,
    session.get(url) as response,
  ):
    logging.info(f'Downloading {url}')
    return await response.text()


def get_cases(content: str, regex: Pattern = CASES_RE) -> int | None:
  if match := regex.search(content):
    match match.groups():
      case [cases] if cases.isnumeric():
        return int(cases)

  return None


async def get_results() -> AsyncIterable[Result]:
  schools = (
    School(name, url)
    for name, url in SCHOOL_URLS.items()
  )

  tasks: list[Awaitable[Result]] = [
    school.get_cases()
    for school in schools
  ]

  for task in as_completed(tasks):
    yield await task


## unused


async def get_results_shared() -> AsyncIterable[Result]:
  async with get_session() as session:
    schools = (
      School(name, url)
      for name, url in SCHOOL_URLS.items()
    )

    tasks: list[Awaitable[Result]] = [
      school.get_cases(session)
      for school in schools
    ]

    for task in as_completed(tasks):
      yield await task


def get_cases_py39(content: str, regex: Pattern = CASES_RE) -> int | None:
  match = regex.search(content)

  if match:
    groups = match.groups()

    if len(groups) == 1:
      [cases] = groups

      if cases.isnumeric():
        return int(cases)

  return None


async def download2(url: str, session: ClientSession | None = None) -> str:
  if session:
    async with limit_downloads, session.get(url) as response:
      logging.info(f'Downloading {url}')
      return await response.text()

  session = get_session()

  async with (
    session,
    limit_downloads,
    session.get(url) as response,
  ):
    logging.info(f'Downloading {url}')
    return await response.text()


async def download_share_session(
  url: str,
  session: ClientSession | None = None
) -> str:
  should_close: bool = False

  if not session:
    session = get_session()
    should_close = True

  async with limit_downloads, session.get(url) as response:
    logging.info(f'Downloading {url}')
    result = await response.text()

  if should_close:
    await session.close()

  return result


async def print_results():
  async for school, cases in get_results():
    if cases is None:
      print(f'Could not retrieve cases for {school.name}.')

    else:
      print(f'{school.name} has {cases} COVID cases.')

