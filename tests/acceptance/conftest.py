"""Configuration for acceptance tests."""

# Mock psycopg2 and related modules before any imports
sys.modules["psycopg2"] = MagicMock()
sys.modules["psycopg2.extras"] = MagicMock()
# Removed pg8000 mock to allow real database connections in integration tests
# sys.modules['pg8000'] = MagicMock()
# sys.modules['pg8000.native'] = MagicMock()

# Mock other potentially problematic modules
sys.modules["sedona"] = MagicMock()
sys.modules["sedona.spark"] = MagicMock()
sys.modules["sedona.spark.SedonaContext"] = MagicMock()
