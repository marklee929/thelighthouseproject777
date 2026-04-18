import logging
import sys
import os

# Build the log directory path and create it if needed.
LOGS_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
APP_LOG_FILE = os.path.join(LOGS_DIR, 'app.log')

# Global application logger configuration.
# This is evaluated once on module load and applied across the application.
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(name)s] [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(APP_LOG_FILE, encoding='utf-8'),  # File handler.
        logging.StreamHandler(sys.stdout)  # Keep console output enabled.
    ]
)

def get_logger(name: str) -> logging.Logger:
    """
    Return a logger with the standard application configuration applied.
    
    Args:
        name (str): Logger name, usually __name__.

    Returns:
        logging.Logger: Configured logger instance.
    """
    return logging.getLogger(name)
