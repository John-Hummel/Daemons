import os
import sys
import json
import time
import signal
import logging
import subprocess
import threading
import tempfile
import ctypes
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Union, Any

try:
    import winreg  # type: ignore
except Exception:  # pragma: no cover - non Windows
    winreg = None  # type: ignore

# basic logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ErrorHandler:
    """Route errors to registered handlers with simple debugging tips."""

    def __init__(self) -> None:
        self.routes: Dict[type, Callable[[Exception], None]] = {}

    def register(self, error_type: type, handler: Callable[[Exception], None]) -> None:
        self.routes[error_type] = handler

    def handle(self, error: Exception) -> None:
        handler = self.routes.get(type(error))
        if handler:
            logger.info("Handling %s with custom handler", type(error).__name__)
            handler(error)
        else:
            logger.error("Unhandled error: %s", error)
            self._provide_debugging_tips(error)
            raise error

    def _provide_debugging_tips(self, error: Exception) -> None:
        if isinstance(error, FileNotFoundError):
            logger.info("Debugging Tip: Check if the file path exists")
        elif isinstance(error, PermissionError):
            logger.info("Debugging Tip: Ensure you have the required permissions")
        elif isinstance(error, subprocess.CalledProcessError):
            logger.info("Debugging Tip: Verify the command and its output")
        elif isinstance(error, KeyError):
            logger.info("Debugging Tip: Check configuration keys")
        elif isinstance(error, ValueError):
            logger.info("Debugging Tip: Validate input values")
        else:
            logger.info("Debugging Tip: See logs for details")


class Scripts:
    """Generate temporary scripts for various languages and execute them."""

    templates: Dict[str, str] = {
        "bash": "#!/bin/bash\n\n{content}",
        "python": "#!/usr/bin/env python3\n\n{content}",
        "powershell": "{content}",
        "ruby": "#!/usr/bin/env ruby\n\n{content}",
        "perl": "#!/usr/bin/env perl\n\n{content}",
    }

    def create_temp_script(self, script_type: str, content: str) -> str:
        if script_type not in self.templates:
            raise ValueError(f"Unsupported script type: {script_type}")
        suffix = {
            "bash": ".sh",
            "python": ".py",
            "powershell": ".ps1",
            "ruby": ".rb",
            "perl": ".pl",
        }.get(script_type, ".txt")
        script_content = self.templates[script_type].format(content=content)
        temp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode="w")
        temp.write(script_content)
        temp.close()
        logger.info("Temporary %s script created at %s", script_type, temp.name)
        return temp.name

    def execute_script(self, script_type: str, content: str, args: Optional[List[str]] = None) -> None:
        path = self.create_temp_script(script_type, content)
        try:
            if script_type == "bash":
                cmd = ["bash", path]
            elif script_type == "python":
                cmd = [sys.executable, path]
            elif script_type == "powershell":
                cmd = ["powershell", "-ExecutionPolicy", "Bypass", "-File", path]
            elif script_type == "ruby":
                cmd = ["ruby", path]
            elif script_type == "perl":
                cmd = ["perl", path]
            else:
                cmd = [path]
            subprocess.run(cmd + (args or []), check=True)
            logger.info("Executed %s script: %s", script_type, path)
        finally:
            os.remove(path)
            logger.info("Deleted temporary %s script: %s", script_type, path)


def Script(func: Callable[..., str]) -> Callable[[str], None]:
    """Decorator to easily create temporary scripts."""

    def wrapper(script_type: str, *args: Any, **kwargs: Any) -> None:
        Scripts().execute_script(script_type, func(*args, **kwargs), args=kwargs.get("args"))

    return wrapper


class BaseService:
    """Common functionality for daemons and watchdogs."""

    def __init__(self, error_handler: Optional[ErrorHandler] = None) -> None:
        self.error_handler = error_handler or ErrorHandler()
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 30
        self._stop_event = threading.Event()

    def update_heartbeat(self) -> None:
        self.last_heartbeat = time.time()

    def is_heartbeat_stale(self) -> bool:
        return time.time() - self.last_heartbeat > self.heartbeat_interval

    def heartbeat_monitor(self, alert_callback: Optional[Callable[[], None]] = None) -> None:
        def loop() -> None:
            while not self._stop_event.is_set():
                time.sleep(self.heartbeat_interval)
                if self.is_heartbeat_stale():
                    logger.warning("%s missed heartbeat", self.__class__.__name__)
                    if alert_callback:
                        alert_callback()
        threading.Thread(target=loop, daemon=True).start()

    def stop(self) -> None:
        self._stop_event.set()


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
    elevate: bool = False
    register: bool = False
    task_scheduler: bool = False
    console_debug: bool = False
    heartbeat: bool = False
    timeout: Optional[int] = None
    target: Optional[str] = None
    config_file: str = "config.json"


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
        popen_cmd: Union[List[str], str] = " ".join(cmd) if self.cfg.shell else cmd
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
        logger.info("Started daemon %s", self.cfg.name)

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
            logger.info("Terminated daemon %s", self.cfg.name)
        self.process = None

    def is_running(self) -> bool:
        return bool(self.process and self.process.poll() is None)

    def autostart(self) -> None:
        if self.cfg.autostart:
            if not self.is_running():
                self.start()
        else:
            logger.info("%s does not require autostart", self.cfg.name)

    def elevate(self) -> None:
        if not self.cfg.elevate:
            return
        logger.info("Elevating privileges for %s", self.cfg.name)
        if os.name == "nt" and winreg:
            ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)  # type: ignore
        elif os.name != "nt" and os.geteuid() != 0:
            subprocess.call(["sudo", sys.executable] + sys.argv)

    def register(self) -> None:
        if not self.cfg.register:
            return
        if os.name == "nt" and winreg:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Run", 0, winreg.KEY_SET_VALUE)
            winreg.SetValueEx(key, self.cfg.name, 0, winreg.REG_SZ, f'"{sys.executable}" "{os.path.abspath(__file__)}"')
            winreg.CloseKey(key)
            logger.info("%s registered in Windows startup", self.cfg.name)
        else:
            logger.info("%s registered in Unix-like startup", self.cfg.name)

    def setup_task_scheduler(self) -> None:
        if not self.cfg.task_scheduler:
            return
        if os.name == "nt":
            subprocess.call([
                "schtasks",
                "/create",
                "/tn",
                self.cfg.name,
                "/tr",
                f'"{sys.executable}" "{os.path.abspath(__file__)}"',
                "/sc",
                "onlogon",
            ])
            logger.info("%s registered in Windows Task Scheduler", self.cfg.name)
        else:
            logger.info("%s registered in Unix-like task scheduler", self.cfg.name)

    def write_config(self, config_data: Dict[str, Any]) -> None:
        with open(self.cfg.config_file, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=4)
        logger.info("Configuration written to %s", self.cfg.config_file)

    def target_check(self) -> None:
        if not self.cfg.target:
            logger.info("No target specified for %s", self.cfg.name)
            return
        if os.path.exists(self.cfg.target):
            logger.info("Target %s exists", self.cfg.target)
        else:
            logger.error("Target %s does not exist", self.cfg.target)

    def heartbeat(self) -> None:
        if self.cfg.heartbeat:
            logger.info("Heartbeat for %s is active", self.cfg.name)
            signal.signal(signal.SIGUSR1, lambda *_: None)
            signal.signal(signal.SIGUSR2, lambda *_: None)
        else:
            logger.info("Heartbeat for %s is not active", self.cfg.name)


@dataclass
class WatchdogConfig:
    name: str
    path: str
    callback: Callable[[str], None]
    timeout: Optional[int] = None


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
            def on_any_event(self, event):
                if not event.is_directory:
                    cfg.callback(event.src_path)

        self._handler = Handler()
        self._observer = Observer()
        self._timeout_thread: Optional[threading.Timer] = None

    def start(self) -> None:
        self._observer.schedule(self._handler, self.cfg.path, recursive=True)
        self._observer.start()
        if self.cfg.timeout:
            self._timeout_thread = threading.Timer(self.cfg.timeout, self.stop)
            self._timeout_thread.start()
        logger.info("Watching %s for changes", self.cfg.path)

    def stop(self) -> None:
        if self._timeout_thread:
            self._timeout_thread.cancel()
            self._timeout_thread = None
        self._observer.stop()
        self._observer.join()
        logger.info("Stopped watching %s", self.cfg.path)

    def heartbeat(self) -> None:
        if self.cfg.timeout:
            logger.info("Heartbeat for %s is active", self.cfg.path)
            signal.signal(signal.SIGUSR1, lambda *_: None)
            signal.signal(signal.SIGUSR2, lambda *_: None)
        else:
            logger.info("Heartbeat for %s is not active", self.cfg.path)


class Signals:
    def __init__(self, stop_handler: Callable[[int, Optional[object]], None], usr1_handler: Optional[Callable] = None, usr2_handler: Optional[Callable] = None):
        self.handlers = {signal.SIGINT: stop_handler, signal.SIGTERM: stop_handler}
        if hasattr(signal, "SIGUSR1") and usr1_handler:
            self.handlers[signal.SIGUSR1] = usr1_handler
        if hasattr(signal, "SIGUSR2") and usr2_handler:
            self.handlers[signal.SIGUSR2] = usr2_handler

    def register(self) -> None:
        for sig, handler in self.handlers.items():
            try:
                signal.signal(sig, handler)
            except ValueError as e:
                logger.error("Failed to register signal %s: %s", sig, e)


class Manager(BaseService):
    """Manage multiple daemons and watchdogs."""

    def __init__(self, config: Union[str, Dict[str, Any]], error_handler: Optional[ErrorHandler] = None, name: str = "Manager") -> None:
        super().__init__(error_handler)
        self.name = name
        if isinstance(config, str):
            with open(config, "r", encoding="utf-8") as f:
                self.config = json.load(f)
        else:
            self.config = config
        self.daemons: List[Daemon] = []
        self.watchdogs: List[Watchdog] = []
        self.signals = Signals(self.stop, self._handle_usr1, self._handle_usr2)
        self._init_services()

    def _handle_usr1(self, signum: int, frame: Optional[object]) -> None:
        logger.info("Received SIGUSR1")

    def _handle_usr2(self, signum: int, frame: Optional[object]) -> None:
        logger.info("Received SIGUSR2")

    def _init_services(self) -> None:
        for d in self.config.get("daemons", []):
            daemon_cfg = DaemonConfig(**d)
            self.daemons.append(Daemon(daemon_cfg, self.error_handler))
        for w in self.config.get("watchdogs", []):
            wcfg = WatchdogConfig(name=w["name"], path=w["path"], callback=lambda p: logger.info("changed: %s", p), timeout=w.get("timeout"))
            self.watchdogs.append(Watchdog(wcfg, self.error_handler))

    def start(self) -> None:
        self.signals.register()
        for d in self.daemons:
            d.elevate()
            d.register()
            d.setup_task_scheduler()
            if d.cfg.autostart:
                d.start()
            if d.cfg.heartbeat:
                d.heartbeat()
        for w in self.watchdogs:
            w.start()
            if w.cfg.timeout:
                w.heartbeat()

    def stop(self, *args: Any, **kwargs: Any) -> None:  # signal handlers may pass args
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

    def write_config(self, config_data: Dict[str, Any]) -> None:
        with open("config.json", "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=4)
        logger.info("Configuration written to config.json")


def main(argv: Optional[List[str]] = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run daemon manager")
    parser.add_argument("config", help="path to JSON configuration file")
    args = parser.parse_args(argv)

    Manager(args.config).run()


if __name__ == "__main__":
    main()
