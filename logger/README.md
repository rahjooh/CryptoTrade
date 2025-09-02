# Logger

This package wraps [logrus](https://github.com/sirupsen/logrus) to provide consistent structured logging across the project.

## Usage

```go
log := logger.GetLogger()
log.WithComponent("example").Info("hello world")
```

## Levels

Logging level is controlled via the `LOG_LEVEL` environment variable. Supported values:

- `debug`
- `info` *(default)*
- `warn`
- `error`
- `report` - prints WARN/ERROR logs and periodic runtime reports

## Format

Logs are emitted in JSON with RFC3339 nanosecond timestamps and include the source file and line number for each entry.

When outputting to a file, log rotation with time-based retention can be configured via the `max_age` field in the `logging`
section of `config/config.yml`. The value represents the number of days to retain old log files.
