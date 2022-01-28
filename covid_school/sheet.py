from typing import (
  Callable, Type, AsyncIterable,
  Final
)
from string import ascii_uppercase
from datetime import datetime
from functools import partial
from os import environ
from asyncio import gather
import asyncio
import logging

from aiostream.stream import starmap  # type: ignore
from asyncstdlib.functools import cache  # type: ignore
from google.oauth2.credentials import Credentials  # type: ignore
from gspread_asyncio import (  # type: ignore
  AsyncioGspreadClient as Client,
  AsyncioGspreadClientManager as ClientManager,
  AsyncioGspreadWorksheet as Worksheet,
  AsyncioGspreadSpreadsheet as Spreadsheet,
)

from .base import (
  get_results, School, T, USER_FILE, set_logging,
  should_dry_run, RELAX_ENV, ENABLED, Result,
  DEFAULT_DRY_RUN, BackgroundTask, background
)


environ[RELAX_ENV] = ENABLED


SCOPES: list[str] = [
  "https://spreadsheets.google.com/feeds",
  "https://www.googleapis.com/auth/spreadsheets",
  "https://www.googleapis.com/auth/drive",
]

TOKEN_FILE: str = environ.get('TOKEN_FILE', USER_FILE)
URL: str = environ.get('SHEET_URL')

WORKSHEET_NAME: str = 'Case Data'

# named ranges
RECENT_COL: str = 'recent'
DATES: str = 'dates'
SCHOOLS: str = 'schools'

NO_CASES = 'Not on website'

DATE_FMT: str = '%-m/%-d/%Y'

# A1 format
A1_START: int = 1
TO_A1: dict[int, str] = {
  num: char
  for num, char in enumerate(ascii_uppercase, start=A1_START)
}
FROM_A1: dict[str, int] = {char: num for num, char in TO_A1.items()}

BLANK_COL: list[list] = [[]]


Api = tuple[ClientManager, Client, Spreadsheet, Worksheet]
CredFunc = Callable[..., Credentials]
SchoolRows = dict[str, int]

Col = int
Row = int
DateCol = Col


# sheet constants
NEXT_COL: Col = 1
NEW_COL: Col = 3

DATE_ROW: Row = 3
DATE_START: DateCol = 3

SCHOOL_START: Row = 4
SCHOOL_COL: Col = 2

FIRST_SCHOOL_ROW: Row = SCHOOL_START
LAST_SCHOOL_ROW: Row = 19

TOTAL_ROW: Row = 20
LAST_UPDATED: tuple[Col, Row] = 2, 2

SUM_FUNC_FMT: str = \
  f'=SUM({{col}}{FIRST_SCHOOL_ROW}:{{col}}{LAST_SCHOOL_ROW})'


ResultQueue = asyncio.Queue[Result]


async def update_raw(
  sheet: Worksheet,
  a1: str,
  content: str,
):
  await asyncio.to_thread(
    sheet.ws.update,
    a1,
    content,
    raw=False,
  )


def get_creds(
  file: str = TOKEN_FILE,
  scopes: list[str] = SCOPES,
) -> Credentials:
  creds = Credentials.from_authorized_user_file(
    file,
    scopes=SCOPES
  )

  return creds


def get_manager(
  func: CredFunc = get_creds
) -> ClientManager:
  manager = ClientManager(get_creds)
  return manager


async def get_client(
  manager: ClientManager
) -> Client:
  return await manager.authorize()


async def get_api() -> Api:
  manager = get_manager()
  client = await get_client(manager)

  spreadsheet = await client.open_by_url(URL)
  sheet = await spreadsheet.worksheet(WORKSHEET_NAME)

  return manager, client, spreadsheet, sheet


async def get_sheet(
  dry_run: bool = DEFAULT_DRY_RUN
) -> Worksheet:
  *_, sheet = await get_api()

  if should_dry_run(dry_run):
    patch(sheet)

  return sheet


async def get_col_schools(sheet: Worksheet) -> tuple[Col, SchoolRows]:
  col = background(get_date_col(sheet))
  schools = background(get_schools(sheet))

  return await gather(col, schools)


async def add_new_col(
  sheet: Worksheet,
  date: str,
  date_col: Col,
):
  # update timestamp in background
  time: BackgroundTask = background(
    sheet.update_cell(*LAST_UPDATED, date)
  )

  # insert new column for data
  await sheet.insert_cols(BLANK_COL, col=date_col)

  # add date to `Dates` row
  await sheet.update_cell(DATE_ROW, date_col, date)

  # ensure timestamp is written
  await time


async def get_date_col(
  sheet: Worksheet
) -> DateCol:
  first: str | None = None
  col: DateCol = NEW_COL

  now = datetime.now()
  date: str = now.strftime(DATE_FMT)

  # see if column already exists
  if vals := await sheet.get_values(DATES):
    [dates] = vals

    if date in dates:
      return col

  # get existing column for most recent date
  if vals := await sheet.get_values(RECENT_COL):
    [first], *_ = vals

  # add new column if latest column is not today's
  if first and date != first:
    await add_new_col(sheet, date, col)

  return col


@cache
async def get_schools(
  sheet: Worksheet
) -> SchoolRows:
  names: list[list[str]] = await sheet.get_values(SCHOOLS)

  return {
    name: index
    for index, [name] in enumerate(names, start=SCHOOL_START)
  }


async def add_result(
  sheet: Worksheet,
  col: Col,
  school: School,
  cases: int | None,
  schools: SchoolRows,
):
  logging.info(f'Adding result <{school.name}: {cases} cases>.')
  row = schools[school.name]

  if cases is None:
    cases: str = NO_CASES

  await sheet.update_cell(row, col, cases)


async def add_results(
  sheet: Worksheet,
  col: Col,
  schools: SchoolRows,
):
  total: BackgroundTask = background(add_total(sheet, col))

  results_gen: AsyncIterable[Result] = get_results()
  add_result_: Callable = \
    partial(add_result, sheet, col, schools=schools)

  tasks = starmap(results_gen, add_result_)

  await tasks
  await total


async def check_and_update(
  sheet: Worksheet,
):
  col, schools = await get_col_schools(sheet)
  await add_results(sheet, col, schools)


async def add_total(
  sheet: Worksheet,
  col: Col,
):
  col_a1: str = TO_A1[col]
  cell = f'{col_a1}{TOTAL_ROW}'
  sum_func: str = SUM_FUNC_FMT.format(col=col_a1)

  await update_raw(sheet, cell, sum_func)


async def _patched_method(self: Type[T], *args, **kwargs):
  logging.info(f'Patched method called with args: {args}, {kwargs}')


def patch(sheet: Worksheet):
  sheet.update_cell = _patched_method
  sheet.insert_cols = _patched_method


async def main(
  dry_run: bool = DEFAULT_DRY_RUN
):
  set_logging()

  sheet = await get_sheet(dry_run)
  await check_and_update(sheet)
