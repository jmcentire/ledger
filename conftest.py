import sys
from pathlib import Path

# Add src/ so "from config import ..." works
sys.path.insert(0, str(Path(__file__).parent / "src"))
# Add root so "from src.config import ..." works
sys.path.insert(0, str(Path(__file__).parent))
