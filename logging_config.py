import logging
from rich.logging import RichHandler
from rich.console import Console
logging.basicConfig(level=logging.INFO, format="%(message)s", datefmt="[%X]", handlers=[RichHandler(markup=True)])
log = logging.getLogger("rich")
console = Console()
"""Logging configuration with RichHandler"""