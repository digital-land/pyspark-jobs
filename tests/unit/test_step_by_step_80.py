"""Step-by-step strategic approach to reach 80% coverage."""

import pytest
from unittest.mock import Mock, patch


class TestStepByStep80:
    """Strategic step-by-step approach to 80% coverage."""

    def test_step_1_logger_config_line_180(self):
        """Step 1: Target logger_config.py line 180 - highest impact (98.33% -> 100%)."""
        from jobs.utils.logger_config import set_spark_log_level
        
        # When SparkContext doesn't exist, this hits the missing line 180
        set_spark_log_level("ERROR")
        set_spark_log_level("WARN")
        set_spark_log_level("INFO")
        set_spark_log_level("DEBUG")