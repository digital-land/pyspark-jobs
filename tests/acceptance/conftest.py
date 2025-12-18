"""Configuration for acceptance tests."""
import sys
from unittest.mock import MagicMock

# Mock psycopg2 and related modules before any imports
sys.modules['psycopg2'] = MagicMock()
sys.modules['psycopg2.extras'] = MagicMock()
sys.modules['pg8000'] = MagicMock()
sys.modules['pg8000.native'] = MagicMock()

# Mock other potentially problematic modules
sys.modules['sedona'] = MagicMock()
sys.modules['sedona.spark'] = MagicMock()
sys.modules['sedona.spark.SedonaContext'] = MagicMock()