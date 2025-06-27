import os
import sys
import subprocess
import ctypes
import threading
import watchdog
import watchdog.events
import watchdog.observers
import time
import logging
import signal
import json
import tempfile
import winreg
import queue
from logging.handlers import QueueHandler, QueueListener

# Initialize a thread-safe logging queue
log_queue = queue.Queue()
queue_handler = QueueHandler(log_queue)
logging.basicConfig(level=logging.INFO, handlers=[queue_handler])
logger = logging.getLogger(__name__)
listener = QueueListener(log_queue, *logging.getLogger().handlers)
listener.start()

class ErrorHandler:
    def __init__(self):
        self.routes = {}

    def register(self, error_type, handler):
        """Register a handler for a specific error type."""
        self.routes[error_type] = handler

    def handle(self, error):
        """Route the error to the appropriate handler and provide debugging tips."""
        error_type = type(error)
        handler = self.routes.get(error_type)
        if handler:
            logging.info(f"Handling {error_type.__name__} with custom handler")
            handler(error)
        else:
            logging.error(f"Unhandled error: {error}")
            self._provide_debugging_tips(error)
            raise error

    def _provide_debugging_tips(self, error):
        """Provide debugging tips based on the error type."""
        if isinstance(error, FileNotFoundError):
            logging.info("Debugging Tip: Check if the file path is correct and the file exists.")
        elif isinstance(error, PermissionError):
            logging.info("Debugging Tip: Ensure you have the necessary permissions to access the resource.")
        elif isinstance(error, subprocess.CalledProcessError):
            logging.info("Debugging Tip: Verify the command being executed and check its output for errors.")
        elif isinstance(error, KeyError):
            logging.info("Debugging Tip: Check if the key exists in the dictionary or configuration.")
        elif isinstance(error, ValueError):
            logging.info("Debugging Tip: Ensure the input value is of the expected type and format.")
        else:
            logging.info("Debugging Tip: Review the stack trace and logs for more details about the error.")

class Scripts:
    def __init__(self):
        self.templates = {
            "bash": "#!/bin/bash\n\n# Temporary Bash Script\n{content}",
            "python": "#!/usr/bin/env python3\n\n# Temporary Python Script\n{content}",
            "powershell": "# PowerShell Script\n{content}",
            "ruby": "#!/usr/bin/env ruby\n\n# Temporary Ruby Script\n{content}",
            "perl": "#!/usr/bin/env perl\n\n# Temporary Perl Script\n{content}",
        }

    def create_temp_script(self, script_type, content):
        """Create a temporary script file based on the specified type and content."""
        if script_type not in self.templates:
            raise ValueError(f"Unsupported script type: {script_type}")

        script_content = self.templates[script_type].format(content=content)
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=self._get_suffix(script_type), mode='w')
        temp_file.write(script_content)
        temp_file.close()
        logging.info(f"Temporary {script_type} script created at {temp_file.name}")
        return temp_file.name

    def _get_suffix(self, script_type):
        """Get the appropriate file extension for the script type."""
        return {
            "bash": ".sh",
            "python": ".py",
            "powershell": ".ps1",
            "ruby": ".rb",
            "perl": ".pl",
        }.get(script_type, ".txt")

    def execute_script(self, script_type, content, args=None):
        """Generate and execute a script with optional arguments."""
        script_path = self.create_temp_script(script_type, content)
        try:
            if script_type == "bash":
                subprocess.run(["bash", script_path] + (args or []), check=True)
            elif script_type == "python":
                subprocess.run([sys.executable, script_path] + (args or []), check=True)
            elif script_type == "powershell":
                subprocess.run(["powershell", "-ExecutionPolicy", "Bypass", "-File", script_path] + (args or []), check=True)
            elif script_type == "ruby":
                subprocess.run(["ruby", script_path] + (args or []), check=True)
            elif script_type == "perl":
                subprocess.run(["perl", script_path] + (args or []), check=True)
            logging.info(f"Executed {script_type} script: {script_path}")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error executing {script_type} script: {e}")
        finally:
            os.remove(script_path)
            logging.info(f"Deleted temporary {script_type} script: {script_path}")

def Script(func):
    """Decorator to define and execute scripts using the @Script syntax."""
    def wrapper(script_type, *args, **kwargs):
        scripts = Scripts()
        content = func(*args, **kwargs)
        scripts.execute_script(script_type, content, args=kwargs.get("args", []))
    return wrapper

import datetime

class BaseService:
    def __init__(self, error_handler=None):
        self.error_handler = error_handler or ErrorHandler()
        self.last_heartbeat = datetime.datetime.now(datetime.timezone.utc)
        self.heartbeat_interval = 30  # seconds, can be overridden

    def update_heartbeat(self):
        self.last_heartbeat = datetime.datetime.now(datetime.timezone.utc)
        logging.debug(f"{self.__class__.__name__} heartbeat updated at {self.last_heartbeat}")

    def is_heartbeat_stale(self):
        return (datetime.datetime.now(datetime.timezone.utc) - self.last_heartbeat).total_seconds() > self.heartbeat_interval

    def heartbeat_monitor(self, alert_callback=None):
        def monitor_loop():
            while True:
                time.sleep(self.heartbeat_interval)
                if self.is_heartbeat_stale():
                    logging.warning(f"{self.__class__.__name__} missed heartbeat!")
                    if alert_callback:
                        alert_callback()
        threading.Thread(target=monitor_loop, daemon=True).start()


class Daemon(BaseService):
    def __init__(self, name, command, args, env=None, cwd=None, log_file=None, cfg=None, error_handler=None):
        self.name = name
        self.command = command
        self.args = args
        self.env = env or os.environ.copy()
        self.cwd = cwd or os.getcwd()
        self.log_file = log_file or os.path.join(self.cwd, f"{self.name}.log")
        self.process = None
        self.cfg = cfg or {}
        self.timeout = self.cfg.get("timeout", None)
        self.console_debug = self.cfg.get("console_debug", False)

    def start(self):
        try:
            use_shell = self.cfg.get("shell", False)
            if use_shell:
                full_cmd = f"{self.command} {' '.join(self.args)}"
                self.process = subprocess.Popen(
                    full_cmd,
                    env=self.env,
                    cwd=self.cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    shell=True,
                    bufsize=1
                )
            else:
                self.process = subprocess.Popen(
                    [self.command] + self.args,
                    env=self.env,
                    cwd=self.cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    shell=False,
                    bufsize=1
                )
            threading.Thread(target=self._monitor_output, daemon=True).start()
        except Exception as e:
            logging.error(f"Failed to start daemon {self.name}: {e}")

    def _monitor_output(self):
        logging.debug(f"Started monitoring output for daemon [{self.name}]")
        try:
            with open(self.log_file, 'a', encoding='utf-8') as log_file:
                while True:
                    if self.process.poll() is not None:
                        logging.debug(f"[{self.name}] process exited with code {self.process.returncode}")
                        break
                    output = self.process.stdout.readline()
                    if output:
                        log_file.write(output)
                        log_file.flush()
                        logging.debug(f"[{self.name}] {output.strip()}")
                        if self.console_debug:
                            print(f"[{self.name}] {output.strip()}")
                    else:
                        time.sleep(1000000)
        except Exception as e:
            logging.error(f"Error in output monitor for {self.name}: {e}")

    def handle_error(self, error):
        logging.error(f"Error in Daemon {self.name}: {error}")
        # Add any additional error handling logic here

    def autostart(self):
        if self.cfg.get('autostart', True):
            if self._is_running():
                logging.info(f"{self.name} is already running")
                return
            self.start()
        else:
            logging.info(f"{self.name} does not require autostart")

    def elevate(self):
        if self.cfg.get('elevate', False):
            if os.name == 'nt':
                ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, " ".join(sys.argv), None, 1)
            elif os.geteuid() != 0:
                subprocess.call(['sudo', sys.executable] + sys.argv)
            logging.info(f"Elevating privileges for {self.name}")
        else:
            logging.info(f"{self.name} does not require elevation")

    def register(self):
        if self.cfg.get('register', False):
            if os.name == 'nt':
                key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Run", 0, winreg.KEY_SET_VALUE)
                winreg.SetValueEx(key, self.name, 0, winreg.REG_SZ, f'"{sys.executable}" "{os.path.abspath(__file__)}"')
                winreg.CloseKey(key)
                logging.info(f"{self.name} registered in Windows startup")
            else:
                logging.info(f"{self.name} registered in Unix-like startup")
        else:
            logging.info(f"{self.name} does not require registration")

    def setup_task_scheduler(self):
        if self.cfg.get('task_scheduler', False):
            if os.name == 'nt':
                subprocess.call([
                    'schtasks', '/create', '/tn', self.name,
                    '/tr', f'"{sys.executable}" "{os.path.abspath(__file__)}"',
                    '/sc', 'onlogon'
                ])
                logging.info(f"{self.name} registered in Windows Task Scheduler")
            else:
                logging.info(f"{self.name} registered in Unix-like task scheduler")
        else:
            logging.info(f"{self.name} does not require task scheduler setup")

    def _is_running(self):
        return False

    def write_config(self, config_data):
        with open(self.cfg.get('config_file', 'config.json'), 'w') as f:
            json.dump(config_data, f, indent=4)
        logging.info(f"Configuration written to {self.cfg.get('config_file', 'config.json')}")

    def terminate(self):
        if self.process is not None:
            self.process.terminate()
            self.process.wait()
            logging.info(f"{self.name} terminated")
            self.process = None
        else:
            logging.info(f"{self.name} is not running")

    def target(self):
        if self.cfg.get('target', None) is not None:
            target = self.cfg['target']
            if os.path.exists(target):
                logging.info(f"Target {target} exists")
            else:
                logging.error(f"Target {target} does not exist")
        else:
            logging.info("No target specified in configuration")
    def heartbeat(self):
        if self.cfg.get('heartbeat', False):
            logging.info(f"Heartbeat for {self.name} is active")
            signal.signal(signal.SIGUSR1, self.heartbeat)
            signal.signal(signal.SIGUSR2, self.heartbeat)
        else:
            logging.info(f"Heartbeat for {self.name} is not active")
    def registerwithmonitor(self):
        self.signals.register_signals()
        logging.info(f"Registering {self.name} with monitor")

    def getstats(self):
        return {
            'name': self.name,
            'command': self.command,
            'args': self.args,
            'env': self.env,
            'cwd': self.cwd,
            'log_file': self.log_file,
            'cfg': self.cfg,
        }

    def execute_temp_script(self, script_type, content):
        scripts = Scripts()
        try:
            script_path = scripts.create_temp_script(script_type, content)
            scripts.execute_script(script_type, content)
        finally:
            if os.path.exists(script_path):
                os.remove(script_path)
                logger.info(f"Temporary script deleted: {script_path}")

class Watchdog(BaseService):
    def __init__(self, name, path, callback, timeout=None, error_handler=None):
        super().__init__(error_handler)
        self.name = name
        self.path = path
        self.callback = callback
        self.timeout = timeout
        self.observer = watchdog.observers.Observer()
        self.event_handler = watchdog.events.FileSystemEventHandler()

        # Register all event handlers
        self.event_handler.on_modified = self._on_modified
        self.event_handler.on_created = self._on_created
        self.event_handler.on_deleted = self._on_deleted
        self.event_handler.on_moved = self._on_moved

        self._timeout_thread = None
        self.signals = Signals(self.stop)

    def registerwithmonitor(self):
        self.signals.register_signals()
        logging.info(f"Registering {self.path} with monitor")

    def getstats(self):
        return {
            'name': self.name,
            'path': self.path,
            'timeout': self.timeout,
            'callback': self.callback.__name__ if callable(self.callback) else str(self.callback),
        }

    def _on_modified(self, event):
        try:
            if event.is_directory:
                return
            logging.info(f"File modified: {event.src_path}")
            self.callback(event.src_path)
        except Exception as e:
            logging.error(f"Error in on_modified callback: {e}")

    def _on_created(self, event):
        try:
            if event.is_directory:
                return
            logging.info(f"File created: {event.src_path}")
            self.callback(event.src_path)
        except Exception as e:
            logging.error(f"Error in on_created callback: {e}")

    def _on_deleted(self, event):
        try:
            if event.is_directory:
                return
            logging.info(f"File deleted: {event.src_path}")
            self.callback(event.src_path)
        except Exception as e:
            logging.error(f"Error in on_deleted callback: {e}")

    def _on_moved(self, event):
        try:
            if event.is_directory:
                return
            logging.info(f"File moved: {event.src_path} to {event.dest_path}")
            self.callback(event.src_path, event.dest_path)
        except Exception as e:
            logging.error(f"Error in on_moved callback: {e}")

    def start(self, retries=3, retry_delay=5):
        for attempt in range(retries):
            try:
                # Schedule the observer with the event handler and path
                self.observer.schedule(self.event_handler, self.path, recursive=True)
                self.observer.start()
                logging.info(f"Watching {self.path} for changes")
                
                # Start timeout thread if timeout is set
                if self.timeout:
                    self._timeout_thread = threading.Timer(self.timeout, self.stop)
                    self._timeout_thread.start()
                break
            except Exception as e:
                logging.error(f"Error starting watchdog for {self.path}: {e}")
                self.handle_error(e)
                if attempt < retries - 1:
                    logging.info(f"Retrying watchdog for {self.path} in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logging.error(f"Failed to start watchdog for {self.path} after {retries} attempts")
                    raise

    def stop(self):
        try:
            if self._timeout_thread:
                self._timeout_thread.cancel()
                self._timeout_thread = None
            self.observer.stop()
            self.observer.join()
            logging.info(f"Stopped watching {self.path}")
        except Exception as e:
            logging.error(f"Error stopping watchdog for {self.path}: {e}")

    def heartbeat(self):
        if self.timeout:
            logging.info(f"Heartbeat for {self.path} is active")
            signal.signal(signal.SIGUSR1, self.heartbeat)
            signal.signal(signal.SIGUSR2, self.heartbeat)
        else:
            logging.info(f"Heartbeat for {self.path} is not active")

    def handle_error(self, error):
        logging.error(f"Error in Watchdog {self.name}: {error}")
        # Add any additional error handling logic here
    def execute_temp_script(self, script_type, content):
        """Generate and execute a temporary script."""
        scripts = Scripts()
        script_path = scripts.create_temp_script(script_type, content)
        try:
            if script_type == "bash":
                subprocess.run(["bash", script_path], check=True)
            elif script_type == "python":
                subprocess.run([sys.executable, script_path], check=True)
            elif script_type == "powershell":
                subprocess.run(["powershell", "-ExecutionPolicy", "Bypass", "-File", script_path], check=True)
            elif script_type == "ruby":
                subprocess.run(["ruby", script_path], check=True)
            elif script_type == "perl":
                subprocess.run(["perl", script_path], check=True)
            logging.info(f"Executed temporary {script_type} script: {script_path}")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error executing temporary {script_type} script: {e}")
        finally:
            os.remove(script_path)
            logging.info(f"Temporary {script_type} script deleted: {script_path}")

import platform

class Manager(BaseService):
    def __init__(self, config_file, error_handler=None,name="Manager"):
        super().__init__(error_handler)
        self.name = name
        self.config_file = config_file
        self.daemons = []
        self.watchdogs = []
        self.signals = Signals(self.stop, self._handle_usr1, self._handle_usr2)
        self.load_config()

    def _handle_usr1(self, signum, frame):
        logger.info("Received SIGUSR1 signal")

    def _handle_usr2(self, signum, frame):
        logger.info("Received SIGUSR2 signal")

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
                for daemon_cfg in config.get('daemons', []):
                    daemon = Daemon(**daemon_cfg, error_handler=self.error_handler)
                    self.daemons.append(daemon)
                for watchdog_cfg in config.get('watchdogs', []):
                    watchdog = Watchdog(**watchdog_cfg, error_handler=self.error_handler)
                    self.watchdogs.append(watchdog)
        except Exception as e:
            logging.error(f"Error loading configuration: {e}")
            self.handle_error(e)

    def start(self):
        try:
            for daemon in self.daemons:
                daemon.start()
                daemon.autostart()
                daemon.elevate()
                daemon.register()
                daemon.setup_task_scheduler()
                daemon.heartbeat()

            for watchdog in self.watchdogs:
                watchdog.start()
                watchdog.heartbeat()
        except Exception as e:
            logging.error(f"Error starting Manager: {e}")
            self.handle_error(e)

    def stop(self):
        try:
            for daemon in self.daemons:
                daemon.terminate()

            for watchdog in self.watchdogs:
                watchdog.stop()
        except Exception as e:
            logging.error(f"Error stopping Manager: {e}")
            self.handle_error(e)

    def run(self):
        self.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
            logging.info("Manager stopped")
            sys.exit(0)

    def write_config(self, config_data):
        with open(self.config_file, 'w') as f:
            json.dump(config_data, f, indent=4)
        logging.info(f"Configuration written to {self.config_file}")
    def daemon_status(self):
        for daemon in self.daemons:
            signal.getsignal(signal.SIGUSR1, daemon.heartbeat)
            signal.getsignal(signal.SIGUSR2, daemon.heartbeat)
            logging.info(f"Daemon {daemon.name} status: {daemon.process.poll()}")
            if daemon.process.poll() is None:
                logging.info(f"{daemon.name} is running")
            else:
                logging.info(f"{daemon.name} is not running")
        
    def watchdog_status(self):
        for watchdog in self.watchdogs:
            logging.info(f"Watchdog {watchdog.path} status: {watchdog.observer.is_alive()}")
            if watchdog.observer.is_alive():
                logging.info(f"{watchdog.path} is monitoring")
            else:
                logging.info(f"{watchdog.path} is not monitoring")
    def listen(self):
        self.signals.register_signals()
        logger.info("Listening for signals")
        while True:
            time.sleep(1)
    def handle_error(self, error):
        logging.error(f"Error in Handler {self.name}: {error}")
        # Add any additional error handling logic here

class Signals:
    def __init__(self, stop_handler, usr1_handler=None, usr2_handler=None):
        self.signal_handlers = {}
        self._setup_signal_handlers(stop_handler, usr1_handler, usr2_handler)

    def _setup_signal_handlers(self, stop_handler, usr1_handler=None, usr2_handler=None):
        self.signal_handlers = {
            signal.SIGINT: stop_handler,
            signal.SIGTERM: stop_handler,
        }

        if hasattr(signal, "SIGUSR1") and usr1_handler:
            self.signal_handlers[signal.SIGUSR1] = usr1_handler
        if hasattr(signal, "SIGUSR2") and usr2_handler:
            self.signal_handlers[signal.SIGUSR2] = usr2_handler

    def register_signals(self):
        for sig, handler in self.signal_handlers.items():
            try:
                signal.signal(sig, handler)
            except ValueError as e:
                logging.error(f"Failed to register signal {sig}: {e}")

# Example of registering error handlers
#error_handler = ErrorHandler()
#error_handler.register(FileNotFoundError, lambda e: logging.error(f"File not found: {e}"))
#error_handler.register(PermissionError, lambda e: logging.error(f"Permission denied: {e}"))
#error_handler.register(subprocess.CalledProcessError, lambda e: logging.error(f"Subprocess error: {e}"))
#error_handler.register(KeyError, lambda e: logging.error(f"Key error: {e}"))
#error_handler.register(ValueError, lambda e: logging.error(f"Value error: {e}"))

# Example usage of @Script
@Script
def Scriptname(args):
    return """
# Example Python Script
import sys
print("Arguments passed:", sys.argv[1:])
"""

# Example usage of @Script with additional scripting languages
@Script
def RubyScript(args):
    return """
# Example Ruby Script
puts "Arguments passed: #{ARGV.join(' ')}"
"""

@Script
def PerlScript(args):
    return """
# Example Perl Script
print "Arguments passed: @ARGV\\n";
"""

# Example of calling the scripts
if __name__ == "__main__":
    # Scriptname("python", args=["arg1", "arg2"])
    # RubyScript("ruby", args=["arg1", "arg2"])
    # PerlScript("perl", args=["arg1", "arg2"])
    pass
