# logging_config.py
import logging
import os
from datetime import datetime

# Configuration
default_log_directory = "logs"
current_date = datetime.now().strftime('%Y-%m-%d')

# Ensure the logs directory exists
logs_directory = os.path.join(os.path.dirname(__file__), default_log_directory)
os.makedirs(logs_directory, exist_ok=True)

# Create the log file in the logs directory
log_file = os.path.join(logs_directory, f'{current_date}.log')

# Set up logging
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
