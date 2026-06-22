# Logging

This project uses Python's standard `logging` module directly.

Library modules should create module loggers only:

```python
import logging

logger = logging.getLogger(__name__)
```

Entry points are responsible for configuring logging after Click has parsed CLI
options:

```python
import logging

logging.basicConfig(
    level=logging.DEBUG if debug else logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(name)s:%(lineno)d - %(message)s",
)

for logger_name in ("boto3", "botocore", "urllib3", "py4j", "pyspark"):
    logging.getLogger(logger_name).setLevel(logging.WARNING)
```

Keep global logging configuration out of `src/jobs`. Job and pipeline code should
emit logs through their module loggers and leave formatting, levels, and
third-party suppression to the entry point.
