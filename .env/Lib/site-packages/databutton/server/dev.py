import asyncio
import hashlib
import logging
import os
import shutil
import signal
import traceback
from asyncio.subprocess import Process
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Union

import anyio
import click
import psutil
from anyio import CancelScope, create_task_group, open_process, open_signal_receiver
from anyio.streams.buffered import BufferedByteReceiveStream
from watchfiles import Change, DefaultFilter, arun_process, awatch

from databutton.decorators.apps.streamlit import StreamlitApp
from databutton.decorators.jobs.schedule import Scheduler
from databutton.utils import get_databutton_components_path
from databutton.utils.build import generate_components, read_artifacts_json
from databutton.version import __version__

logger = logging.getLogger("databutton.start")
awatch_logger = logging.getLogger("watchfiles.main")
awatch_logger.setLevel(logging.CRITICAL)


class DatabuttonFilter(DefaultFilter):
    def __init__(
        self,
        *,
        ignore_paths: Optional[Sequence[Union[str, Path]]] = None,
        extra_extensions: Sequence[str] = (),
        include_artifacts_json: bool = False,
    ) -> None:
        """
        Args:
            ignore_paths: The paths to ignore, see [`BaseFilter`][watchfiles.BaseFilter].
            extra_extensions: extra extensions to ignore.

        `ignore_paths` and `extra_extensions` can be passed as arguments partly to support [CLI](../cli.md) usage where
        `--ignore-paths` and `--extensions` can be passed as arguments.
        """
        self.extensions = (".py", ".pyx", ".pyd", ".pyc") + tuple(extra_extensions)
        self.include_artifacts_json = include_artifacts_json
        super().__init__(
            ignore_paths=ignore_paths,
            ignore_dirs=self.ignore_dirs + tuple([".databutton"]),
        )

    def __call__(self, change: "Change", path: str) -> bool:
        ret = (
            path.endswith(self.extensions)
            and super().__call__(change, path)
            and not path.endswith("artifacts.json")
        )
        if self.include_artifacts_json:
            ret = ret or path.endswith("artifacts.json")

        return ret


def get_components_hash():
    p = get_databutton_components_path()
    if not p.exists():
        return False
    md5 = hashlib.md5()
    with open(p, "r") as f:
        md5.update(f.read().encode("utf-8"))
    return md5.hexdigest()


class ComponentsJsonFilter(DefaultFilter):
    def __init__(self, starting_hash: str = None) -> None:
        super().__init__()
        self.prev_hash: Optional[str] = starting_hash

    def __call__(self, change: "Change", path: str) -> bool:
        should_call = super().__call__(change, path) and path.endswith("artifacts.json")
        if should_call:
            if not Path(path).exists():
                # Ignore if the file doesn't exist
                return False
            # Check hash extra check
            digest = get_components_hash()
            if digest == self.prev_hash:
                return False
            self.prev_hash = digest
            return True


class CouldNotStartError(Exception):
    pass


_streamlit_args = [
    ("server.address", "0.0.0.0"),
    ("server.enableWebsocketCompression", "true"),
    ("browser.gatherUsageStats", "false"),
    ("global.dataFrameSerialization", "arrow"),
    ("server.headless", "true"),
    ("server.enableCORS", "false"),
    ("server.enableXsrfProtection", "false"),
    ("server.runOnSave", "true"),
]


class StreamlitWatcher:
    def __init__(self):
        self.apps: Dict[str, StreamlitApp] = {}
        self.processes: Dict[str, Process] = {}

    def get_stable_app_id(self, app: StreamlitApp):
        return f"{app.uid}-{app.port}"

    async def clear(self):
        await self.cancel()
        self.apps = {}
        self.processes = {}

    async def cancel(self):
        async with create_task_group() as tg:
            for key, process in self.processes.items():
                tg.start_soon(self.stop_process, process, self.apps.get(key))

    async def start_process(self, uid: str, app: StreamlitApp) -> Process:
        logger.debug(f"Starting process for {app.name}")
        cmd = (
            f"streamlit run {app.filename} "
            + f"--server.port={app.port} "
            + " ".join([f"--{k}={v}" for k, v in _streamlit_args])
        )
        # Set environment and force PYTHONPATH
        current_env = os.environ.copy()
        current_env["PYTHONPATH"] = "."
        logger.debug(f"Starting:\n{cmd}")
        process = await open_process(cmd, env=current_env)
        logger.debug(f"Started streamlit process on {process.pid}")
        self.processes[uid] = process
        buffered = BufferedByteReceiveStream(process.stdout)
        try:
            streamlit_output = await buffered.receive_exactly(100)
            streamlit_output_str = str(repr(streamlit_output))
            if (
                "You can now view your Streamlit app in your browser"
                not in streamlit_output_str
            ):
                raise Exception("Error in starting streamlit")
            logger.debug(streamlit_output_str)
        except (anyio.EndOfStream, anyio.IncompleteRead):
            error = await process.stderr.receive()
            logger.info(f"Could not start streamlit app {app.name} on port {app.port}")
            logger.info(repr(error))
            raise CouldNotStartError(repr(error))

        return process

    async def stop_process(self, process: Process, app: StreamlitApp):
        try:
            logger.debug(f"Killing process {process.pid}")
            process.kill()
            await process.wait()
            logger.debug(f"Stopped process {process.pid}")
        except:  # noqa
            logger.debug(f"Could not terminate process for {process.pid}.")
            logger.debug(traceback.format_exc())
            # Ignore terminations, we'll nuke them all down below anyway

        # Streamlit has dangling processes, so let's find them and killem if need be
        for psprocess in psutil.process_iter():
            try:
                try:
                    cmdline = psprocess.cmdline()
                except (psutil.AccessDenied, psutil.NoSuchProcess, ProcessLookupError):
                    continue
                if app.filename in cmdline:
                    logger.debug(f"Killing psprocess {psprocess.pid}")
                    psprocess.kill()
                    psprocess.wait(2)
                    logger.debug(f"Forcefully stopped psutil.process {psprocess.pid}")
            except:  # noqa
                logger.debug(
                    f"Could not terminate psutil.process {psprocess.pid}. "
                    + f"{psprocess}",
                )

    async def update_processes_from_apps(
        self, apps: List[StreamlitApp]
    ) -> List[asyncio.Task]:
        apps_map: Dict[str, StreamlitApp] = {
            self.get_stable_app_id(app): app for app in apps
        }
        previous = self.apps
        previous_apps_map = self.apps.copy()
        self.apps = apps_map
        old, new = set(uid for uid in previous.keys()), set(
            uid for uid in apps_map.keys()
        )
        new_apps = list(new - old)
        deleted_apps = list(old - new)

        async with create_task_group() as tg:
            for new_uid in new_apps:
                tg.start_soon(self.start_process, new_uid, apps_map.get(new_uid))

            for deleted_uid in deleted_apps:
                tg.start_soon(
                    self.stop_process,
                    self.processes.get(deleted_uid),
                    previous_apps_map.get(deleted_uid),
                )

            for running_uid in new & old:
                new_app = apps_map.get(running_uid)
                old_app = previous_apps_map.get(running_uid)
                if old_app.uid != new_app.uid:
                    # This has a new port, we should restart it.
                    tg.start_soon(
                        self.stop_process, self.processes.get(running_uid), old_app
                    )
                    tg.start_soon(self.start_process, running_uid, new_app)

        return len(new_apps) > 0 or len(deleted_apps) > 0


@dataclass
class DatabuttonConfig:
    port: int = os.environ.get("PORT", 8000)
    log_level: str = os.environ.get("LOG_LEVEL", "critical")


class GracefulExit(SystemExit):
    code = 1


class DatabuttonRunner:
    def __init__(self, root_dir=Path.cwd(), **config):
        self.root_dir = root_dir
        self.config = DatabuttonConfig(**config)
        self.initial_hash: str = None

    async def create_webwatcher(self):
        args = [("port", self.config.port), ("log-level", self.config.log_level)]
        args_string = " ".join([f"--{arg}={value}" for arg, value in args])
        target_str = f"uvicorn {args_string} databutton.server.prod:app"
        return await arun_process(
            self.root_dir,
            target=target_str,
            target_type="command",
            watch_filter=ComponentsJsonFilter(starting_hash=self.initial_hash),
            callback=lambda _: click.secho("Restarting webserver..."),
        )

    async def create_streamlit_watcher(self):
        streamlit_watcher = StreamlitWatcher()
        self.streamlit_watcher = streamlit_watcher
        components = read_artifacts_json()
        await streamlit_watcher.update_processes_from_apps(components.streamlit_apps)

        async for _ in awatch(
            self.root_dir,
            watch_filter=ComponentsJsonFilter(starting_hash=self.initial_hash),
        ):
            logger.debug("Restarting streamlit apps...")
            new_components = read_artifacts_json()
            await streamlit_watcher.clear()
            await streamlit_watcher.update_processes_from_apps(
                new_components.streamlit_apps
            )

    async def create_scheduler_watcher(self):
        return await arun_process(
            self.root_dir,
            watch_filter=DatabuttonFilter(include_artifacts_json=True),
            target=Scheduler.create,
            callback=lambda _: click.secho("Restarting scheduler..."),
        )

    async def create_components_watcher(self):
        return await arun_process(
            self.root_dir,
            watch_filter=DatabuttonFilter(),
            target=partial(generate_components, self.root_dir),
        )

    async def signal_handler(self, scope: CancelScope):
        with open_signal_receiver(signal.SIGINT) as signals:
            async for _ in signals:
                click.secho("\nshutting down...", fg="cyan")
                await self.streamlit_watcher.clear()
                scope.cancel()
                logger.debug("asyncio scope cancelled")

    async def run(self, debug=False):
        if debug:
            logger.setLevel(logging.DEBUG)
        click.secho(
            f"starting databutton {click.style(f'v{__version__}', fg='green')}",
            fg="cyan",
        )
        shutil.rmtree(Path(".databutton"), ignore_errors=True)
        generate_components(self.root_dir)
        self.initial_hash = get_components_hash()
        try:
            async with create_task_group() as tg:
                tg.start_soon(self.signal_handler, tg.cancel_scope)
                tg.start_soon(self.create_streamlit_watcher, name="streamlit")
                tg.start_soon(self.create_components_watcher, name="components")
                tg.start_soon(self.create_scheduler_watcher, name="schedule")
                tg.start_soon(self.create_webwatcher, name="web")
        finally:
            logger.debug("Exited cleanly.")
