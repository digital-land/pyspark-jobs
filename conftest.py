"""Root conftest to ensure src is in path."""
import sys
from pathlib import Path

# Add src to Python path at the very beginning
project_root = Path(__file__).parent.absolute()
src_path = str(project_root / 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)
