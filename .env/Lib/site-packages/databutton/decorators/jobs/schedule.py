import datetime
import importlib
import inspect
import logging
import os
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, List
from uuid import uuid4

import anyio
import schedule
from anyio import create_task_group
from dataclasses_json import dataclass_json

from databutton.decorators.jobs.logger import JobRunLogger
from databutton.decorators.jobs.utils import iso_utc_timestamp
from databutton.utils import is_running_locally

if TYPE_CHECKING:
    from databutton.utils.build import ArtifactDict

logger = logging.getLogger("databutton.schedule")


@dataclass
class DatabuttonSchedule:
    # Unique identifier
    uid: str
    # The human readable name for the job
    name: str
    # The actual name of the function
    func_name: str
    # Seconds between runs (first run will be at t0 + seconds)
    seconds: float
    # The path to the .py file where the schedule is defined
    filepath: str
    # The name of the module (i.e jobs.schedule)
    module_name: str
    # Should the job be cancelled on failure
    cancel_on_failure: bool = False
    # Should run immediately after start, then be scheduled
    run_immediately: bool = True

    def to_dict(self):
        return self.to_dict()


_schedules: List[DatabuttonSchedule] = []


def repeat_every(
    seconds: float,
    name: str = None,
    cancel_on_failure: bool = False,
    run_immediately: bool = True,
):
    def wrapper(func):
        filepath = Path(inspect.getfile(func)).relative_to(Path.cwd())
        job_name = name if name is not None else func.__name__
        uid = f"{filepath}-{func.__name__}-{job_name}"
        _schedules.append(
            DatabuttonSchedule(
                uid=uid,
                name=job_name,
                func_name=func.__name__,
                seconds=seconds,
                filepath=str(filepath),
                module_name=inspect.getmodule(func).__name__,
                cancel_on_failure=cancel_on_failure,
                run_immediately=run_immediately,
            )
        )
        return func

    return wrapper


@dataclass_json
@dataclass
class JobRun:
    timestamp: datetime.datetime
    result: Any


class Job(schedule.Job):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.databutton_schedule: DatabuttonSchedule = None

    def from_databutton_schedule(self, databutton_schedule: DatabuttonSchedule):
        self.databutton_schedule = databutton_schedule

        mod = importlib.import_module(databutton_schedule.module_name)
        func = getattr(mod, databutton_schedule.func_name)

        self.uid = databutton_schedule.uid
        self.name = databutton_schedule.name
        return self.do(func)

    async def run(self):
        run_id = str(uuid4())
        if self.databutton_schedule is None:
            job_id = None  # Raising instead because we need a job id below here!
        else:
            job_id = self.databutton_schedule.uid

        if job_id is None and not is_running_locally():
            raise Exception(
                "Running job without a databutton schedule, need a job id to continue"
            )

        if self._is_overdue(datetime.datetime.now()):
            logger.debug("Cancelling job %s", self)
            return schedule.CancelJob

        with JobRunLogger(logger, job_id=job_id, run_id=run_id) as jobrun:
            try:
                if inspect.iscoroutinefunction(self.job_func):
                    ret = await self.job_func()
                else:
                    ret = self.job_func()
                jobrun.success = True
            except Exception as e:
                jobrun.success = False

                # This needs to be printed, as the stdout is handled by JobRunner
                # Otherwise we would need to configure the thing to do standard out
                print(traceback.format_exc())

                if os.environ.get("SENTRY_DSN"):
                    import sentry_sdk

                    with sentry_sdk.push_scope() as scope:
                        scope.set_tag("job.uid", job_id)
                        scope.set_tag("job.run_id", run_id)
                        # Not bothering with fixing this now but I think these are wrong:
                        # This will be when the previous run started?
                        scope.set_extra("job.last_run", self.last_run)
                        # This will be when this run was supposed to start?
                        scope.set_extra("job.next_run", self.next_run)
                        sentry_sdk.capture_exception(e)

                if self.databutton_schedule.cancel_on_failure:
                    ret = schedule.CancelJob
                else:
                    ret = e  # Or None? Depends on how we use the jobrun history later

            self.last_run = jobrun.start_time

            self._schedule_next_run()

            if self._is_overdue(datetime.datetime.now()):
                logger.debug("Cancelling job %s", self)
                return schedule.CancelJob

            # Used on jobrun exit, not set if the cancel above hits
            # (but I'm not sure I follow the original logic here)
            jobrun.next_run_time = iso_utc_timestamp(self.next_run)

        return ret


class Scheduler(schedule.Scheduler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.jobs: List[Job] = []
        self.running_process = None

    def load_schedules(self, components: "ArtifactDict"):
        should_run_immediately: List[Job] = []
        for sched in components.schedules:
            job = self.every(sched.seconds).seconds.from_databutton_schedule(sched)

            if sched.run_immediately:
                should_run_immediately.append(job)

        async def run_continously():
            await self.run_pending(override=should_run_immediately)
            while True:
                await self.run_pending()
                await anyio.sleep(1)

        return run_continously()

    # Have a proxy so we can have a clean coroutine
    async def _create(self, components):
        await self.load_schedules(components)

    @classmethod
    def create(cls):
        from databutton.utils.build import read_artifacts_json

        sys.path.append(".")

        sched = Scheduler()
        components = read_artifacts_json()
        try:
            anyio.run(sched._create, components, backend="trio")
        except KeyboardInterrupt as e:
            # Exit gracefully it interrupted
            logger.debug(e)
            sys.exit(0)
        return sched

    async def run_pending(self, override: List[Job] = None):
        job_pool = override if override else self.jobs
        force = True if override else False
        runnable_jobs = [job for job in job_pool if job.should_run or force]
        async with create_task_group() as tg:
            for job in sorted(runnable_jobs):
                tg.start_soon(job.run)

    def every(self, interval=1):
        job = Job(interval, self)
        return job
