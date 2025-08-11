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

## Format

Logs are emitted in JSON with RFC3339 nanosecond timestamps and include the source file and line number for each entry.
