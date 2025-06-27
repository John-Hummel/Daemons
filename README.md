# Daemon Framework

This repository contains a small daemon/watchdog framework. Use `Manager` to load services from a configuration dictionary or JSON file and keep them running.

## Command line

```
python -m daemon_framework <config.json>
```

Example configuration:

```json
{
  "daemons": [
    {"name": "ping", "command": "ping", "args": ["-c", "1", "127.0.0.1"], "autostart": true}
  ],
  "watchdogs": [
    {"name": "tmpwatch", "path": "/tmp"}
  ]
}
```

## Library

Create your configuration and run the manager:

```
from daemon_framework import Manager

# load from file
Manager("config.json").run()

# or pass a dictionary directly
config = {
    "daemons": [
        {"name": "ping", "command": "ping", "args": ["-c", "1", "localhost"], "autostart": True}
    ]
}

Manager(config).run()
```
