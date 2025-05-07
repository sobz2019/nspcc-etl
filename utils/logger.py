# Logger configuration
import logging
import os
from datetime import datetime
from config.settings import LOGS_DIR

def setup_logger(name=None):
    """
    Configure logger with timestamp in filename and console output
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"etl_{timestamp}.log" if not name else f"{name}_{timestamp}.log"
    log_path = os.path.join(LOGS_DIR, log_filename)
    
    # Create a logger
    logger = logging.getLogger(name or 'nspcc_etl')
    logger.setLevel(logging.INFO)
    
    # Create file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create formatter and add it to handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger