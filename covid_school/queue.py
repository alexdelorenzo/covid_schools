from typing import Final, NamedTuple
from asyncio import Task, gather, Queue

from gspread_asyncio import (  # type: ignore
  AsyncioGspreadWorksheet as Worksheet,
)

from .base import (
  get_results, Result, DEFAULT_DRY_RUN,
  BackgroundTask, background
)
from .sheet import (
  Col, SchoolRows, add_result, get_col_schools,
  get_sheet, add_total
)


DONE: Final[None] = None


ResultQueue = Queue[Result]


class ApiTasks(NamedTuple):
  # api
  col: Col
  sheet: Worksheet

  # tasks
  producer: BackgroundTask
  consumer: BackgroundTask


async def produce(queue: ResultQueue):
  async for result in get_results():
    await queue.put(result)

  await queue.put(DONE)


async def consume(
  queue: ResultQueue,
  sheet: Worksheet,
  col: Col,
  schools: SchoolRows,
):
  tasks: list[Task] = []

  while result := await queue.get():
    task: BackgroundTask = background(
      add_result(sheet, col, *result, schools)
    )
    tasks.append(task)

    queue.task_done()

  await gather(*tasks)
  queue.task_done()


async def get_api_tasks(
  dry_run: bool = DEFAULT_DRY_RUN,
) -> ApiTasks:
  queue = ResultQueue()
  producer: BackgroundTask = background(produce(queue))

  sheet = await get_sheet(dry_run)
  col, schools = await get_col_schools(sheet)

  consumer: BackgroundTask = background(
    consume(queue, sheet, col, schools)
  )

  return ApiTasks(col, sheet, producer, consumer)


# produce() runs in bg before consume() is ready
async def download_cases_add_results(dry_run: bool = DEFAULT_DRY_RUN):
  col, sheet, producer, consumer = \
    await get_api_tasks(dry_run)

  # add total formula
  total: BackgroundTask = background(add_total(sheet, col))

  # wait for tasks to complete
  await gather(producer, consumer, total)


# produce() runs in the bg only when consume() is ready.
# this is equivalent .sheet.add_results(), prefer download_cases_add_results().
# it's cleaner, though.
async def producer_consumer(
  sheet: Worksheet,
  col: Col,
  schools: SchoolRows,
):
  queue = ResultQueue()

  producer: BackgroundTask = background(produce(queue))
  consumer: BackgroundTask = background(
    consume(queue, sheet, col, schools)
  )

  await gather(
    producer,
    consumer,
    queue.join()
  )
