import logging
from contextlib import redirect_stdout

from databutton.decorators.jobs.utils import (
    create_job_run,
    iso_utc_timestamp_now,
    push_job_log,
    update_job_run,
)
from databutton.utils import is_running_locally

logger = logging.getLogger("databutton.logpusher")


class JobRunLogger:
    def __init__(
        self,
        logger: logging.Logger,
        job_id: str,
        run_id: str,
        level: int = logging.INFO,
    ):
        self.logger = logger
        self.level = level
        self.job_id = job_id
        self.run_id = run_id
        self.success = False
        self.start_time = None
        self.end_time = None
        self.next_run_time = None
        self._redirector = redirect_stdout(self)

    def write(self, msg: str):
        # from https://johnpaton.net/posts/redirect-logging/
        if msg and not msg.isspace():
            stripped = msg.strip()
            if not is_running_locally():
                # deployed version
                try:
                    push_job_log(
                        run_id=self.run_id,
                        msg=stripped,
                    )
                except Exception:
                    import traceback

                    logger.error(traceback.format_exc())
            self.logger.log(self.level, stripped)

    def __enter__(self):
        self._redirector.__enter__()
        self.start_time = iso_utc_timestamp_now()
        if not is_running_locally():
            create_job_run(self.job_id, self.run_id, self.start_time)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.end_time = iso_utc_timestamp_now()
        if not is_running_locally():
            update_job_run(
                self.job_id,
                self.run_id,
                self.start_time,
                self.end_time,
                self.next_run_time,
                self.success,
            )
        # let contextlib do any exception handling here
        self._redirector.__exit__(exc_type, exc_value, traceback)

    def flush(self):
        pass
