import os
import sys
import json
import time
import signal
import logging
import subprocess
import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Union, Any

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ErrorHandler:
    """Simple error routing system."""

    def __init__(self) -> None:
        self.routes: Dict[type, Callable[[Exception], None]] = {}

    def register(self, error_type: type, handler: Callable[[Exception], None]) -> None:
        self.routes[error_type] = handler

    def handle(self, error: Exception) -> None:
        handler = self.routes.get(type(error))
        if handler:
            handler(error)
        else:
            logger.error("Unhandled error: %s", error)
            raise error


@dataclass
class DaemonConfig:
    name: str
    command: str
    args: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=lambda: os.environ.copy())
    cwd: str = field(default_factory=os.getcwd)
    log_file: Optional[str] = None
    autostart: bool = True
    shell: bool = False
    console_debug: bool = False


class BaseService:
    """Common features for daemons and watchdogs."""

    def __init__(self, error_handler: Optional[ErrorHandler] = None) -> None:
        self.error_handler = error_handler or ErrorHandler()
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()


class Daemon(BaseService):
    def __init__(self, cfg: DaemonConfig, error_handler: Optional[ErrorHandler] = None) -> None:
        super().__init__(error_handler)
        self.cfg = cfg
        self.process: Optional[subprocess.Popen] = None

    @property
    def log_path(self) -> str:
        return self.cfg.log_file or os.path.join(self.cfg.cwd, f"{self.cfg.name}.log")

    def start(self) -> None:
        if self.process:
            return
        cmd = [self.cfg.command] + self.cfg.args
        popen_cmd = " ".join(cmd) if self.cfg.shell else cmd
        self.process = subprocess.Popen(
            popen_cmd,
            env=self.cfg.env,
            cwd=self.cfg.cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            shell=self.cfg.shell,
            bufsize=1,
        )
        threading.Thread(target=self._monitor_output, daemon=True).start()

    def _monitor_output(self) -> None:
        assert self.process and self.process.stdout
        with open(self.log_path, "a", encoding="utf-8") as log_file:
            for line in iter(self.process.stdout.readline, ""):
                log_file.write(line)
                log_file.flush()
                if self.cfg.console_debug:
                    print(f"[{self.cfg.name}] {line.rstrip()}")
            self.process.wait()

    def terminate(self) -> None:
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.process.wait()
        self.process = None

    def is_running(self) -> bool:
        return bool(self.process and self.process.poll() is None)


@dataclass
class WatchdogConfig:
    name: str
    path: str
    callback: Callable[[str], None]


class Watchdog(BaseService):
    def __init__(self, cfg: WatchdogConfig, error_handler: Optional[ErrorHandler] = None) -> None:
        super().__init__(error_handler)
        self.cfg = cfg
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler
        except Exception as e:  # pragma: no cover - missing dependency
            raise RuntimeError("watchdog package required") from e

        class Handler(FileSystemEventHandler):
            def on_created(self, event):
                if not event.is_directory:
                    cfg.callback(event.src_path)

            on_modified = on_created
            on_moved = on_created
            on_deleted = on_created

        self._handler = Handler()
        self._observer = Observer()

    def start(self) -> None:
        self._observer.schedule(self._handler, self.cfg.path, recursive=True)
        self._observer.start()

    def stop(self) -> None:
        self._observer.stop()
        self._observer.join()


class Signals:
    def __init__(self, stop_handler: Callable[[int, Optional[object]], None]):
        self.stop_handler = stop_handler

    def register(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self.stop_handler)


class Manager(BaseService):
    """Manage multiple daemons and watchdogs."""

    def __init__(self, config: Union[str, Dict[str, Any]], error_handler: Optional[ErrorHandler] = None) -> None:
        super().__init__(error_handler)
        if isinstance(config, str):
            with open(config, "r", encoding="utf-8") as f:
                self.config = json.load(f)
        else:
            self.config = config
        self.daemons: List[Daemon] = []
        self.watchdogs: List[Watchdog] = []
        self.signals = Signals(self._handle_stop)
        self._init_services()

    def _handle_stop(self, signum: int, frame: Optional[object]) -> None:
        self.stop()

    def _init_services(self) -> None:
        for d in self.config.get("daemons", []):
            daemon_cfg = DaemonConfig(**d)
            daemon = Daemon(daemon_cfg, self.error_handler)
            self.daemons.append(daemon)
        for w in self.config.get("watchdogs", []):
            wcfg = WatchdogConfig(
                name=w["name"], path=w["path"], callback=lambda p: logger.info(f"changed: {p}")
            )
            self.watchdogs.append(Watchdog(wcfg, self.error_handler))

    def start(self) -> None:
        self.signals.register()
        for d in self.daemons:
            if d.cfg.autostart:
                d.start()
        for w in self.watchdogs:
            w.start()

    def stop(self) -> None:
        for d in self.daemons:
            d.terminate()
        for w in self.watchdogs:
            w.stop()
        super().stop()

    def run(self) -> None:
        self.start()
        try:
            while not self._stop_event.is_set():
                time.sleep(0.5)
        finally:
            self.stop()



def main(argv: Optional[List[str]] = None) -> None:
    """Entry point for command line usage."""
    import argparse

    parser = argparse.ArgumentParser(description="Run daemon manager")
    parser.add_argument("config", help="path to JSON configuration file")
    args = parser.parse_args(argv)

    Manager(args.config).run()


if __name__ == "__main__":
    main()

