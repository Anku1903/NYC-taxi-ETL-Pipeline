
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger("NYC_Taxi_ETL")

def log_info(message: str):
    logger.info(message)

def log_error(message: str):
    logger.error(message)

def log_warning(message: str):
    logger.warning(message)
