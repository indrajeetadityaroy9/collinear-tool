import sys
import types
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if 'app' not in sys.modules:
    package = types.ModuleType('app')
    package.__path__ = [str(PROJECT_ROOT / 'app')]
    sys.modules['app'] = package