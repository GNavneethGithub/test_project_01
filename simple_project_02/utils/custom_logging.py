# my_main_project_folder/utils/custom_logging.py

import logging
import datetime
import pytz
import sys

# --- Part 1: The Custom Formatter ---
class MultiZoneFormatter(logging.Formatter):
    """
    A custom formatter to create multi-line, multi-timezone logs.
    """
    
    def __init__(self):
        super().__init__()
        # Define the timezones we need
        self.timezones = {
            "UTC": pytz.utc,
            "America/Los_Angeles": pytz.timezone('America/Los_Angeles'),
            "India": pytz.timezone('Asia/Kolkata')
        }

    def format(self, record):
        """
        This method is called for every log message.
        'record' contains all log information.
        """
        
        # 1. Create the timestamp string
        timestamp_str_parts = []
        
        # Get the original log time as a UTC datetime object
        dt_utc = datetime.datetime.fromtimestamp(record.created, tz=pytz.utc)
        
        # Generate the timestamp for each of our required timezones
        for label, tz in self.timezones.items():
            dt_zoned = dt_utc.astimezone(tz)
            # Format to ISO 8601 with offset
            ts = dt_zoned.isoformat() 
            timestamp_str_parts.append(f"[{label}: {ts}]")
            
        timestamp_str = " ".join(timestamp_str_parts)

        # 2. Safely get the custom data passed in 'extra'
        keyword = getattr(record, 'keyword', 'N/A')
        call_chain = getattr(record, 'call_chain', '[]')
        extra_details = getattr(record, 'extra_details', {})

        # 3. Get the main log message
        message = record.getMessage()

        # 4. Handle exceptions (stack traces)
        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)

        # 5. Build the final, multi-line string
        log_entry = (
            # "==========================================\n"
            "\n"
            f"Keyword: {keyword}\n"
            f"logging timestamp: {timestamp_str}\n"
            f"chain of function call: {call_chain}\n"
            f"message: {message}\n"
            f"extra details: {extra_details}\n\n"
            # "=========================================="
        )
        
        return log_entry


# --- Part 2: The Logger Wrapper (Helper Class) ---

class CustomChainLogger:
    """
    A wrapper to make passing the call chain easy.
    """
    
    def __init__(self, logger, chain=None):
        self._logger = logger
        self.chain = chain if chain is not None else []

    def new_frame(self, func_name):
        """
        Creates a *new* logger object with the function name added to the chain. 
        """
        new_chain = [func_name] + self.chain
        return CustomChainLogger(self._logger, new_chain)

    def _log(self, level, keyword, msg, exc_info=False, **kwargs): 
        """Internal log method."""
        
        # Format the chain string
        chain_str = " <- ".join(self.chain)
        
        # Pack all custom data into the 'extra' dict
        extra_data = {
            'keyword': keyword,
            'call_chain': f"[{chain_str}]",
            'extra_details': kwargs  # All other kwargs are a bonus
        }
        
        # Call the actual logger
        self._logger.log(level, msg, extra=extra_data, exc_info=exc_info) 

    # --- Public log methods ---

    
    def debug(self, keyword, msg, **kwargs): 
        self._log(logging.DEBUG, keyword, msg, **kwargs) 
        
    def info(self, keyword, msg, **kwargs): 
        self._log(logging.INFO, keyword, msg, **kwargs) 
        
    def warning(self, keyword, msg, **kwargs):
        self._log(logging.WARNING, keyword, msg, **kwargs)

    def error(self, keyword, msg, exc_info=False, **kwargs): 
        self._log(logging.ERROR, keyword, msg, exc_info=exc_info, **kwargs) 
        
    def critical(self, keyword, msg, exc_info=False, **kwargs): 
        self._log(logging.CRITICAL, keyword, msg, exc_info=exc_info, **kwargs) 

# --- Part 3: The Setup Function ---
def setup_logger():
    """
    Configures and returns the base logger.
    """
    # Get the root logger
    logger = logging.getLogger("my_custom_logger")
    logger.setLevel(logging.DEBUG) # Process all log levels
    
    # Prevent logs from propagating to the default root logger
    logger.propagate = False

    if logger.hasHandlers():
        for handler in list(logger.handlers):
            logger.removeHandler(handler)

    # Create a handler (e.g., to console)
    handler = logging.StreamHandler(sys.stdout)
    
    # Set our new custom formatter
    formatter = MultiZoneFormatter()
    handler.setFormatter(formatter)
    
    # Add the handler to the logger
    logger.addHandler(handler)
    
    return logger








# --- Example Usage ---
def f3(log):
    # We are in f3. Create a new log frame.
    log = log.new_frame("f3")
    
    log.warning(
        "F3_CACHE_CHECK", 
        "A non-critical issue occurred.",
        warning_code="W-005",
        message="Cache miss"
    )
    
    # Simulate an error
    try:
        x = 1 / 0
    except ZeroDivisionError:
        log.error(
            "F3_DIVIDE_ERROR",
            "Failed to divide by zero.",
            exc_info=True, # This tells the logger to add the stack trace
            culprit_variable="x"
        )

def f2(log):
    # We are in f2. Create a new log frame.
    log = log.new_frame("f2")
    
    log.debug(
        "F2_PROCESSING",
        "Processing data in f2.",
        user_id=987,
        transaction_id="abc-123",
        item_count=5
    )
    
    # Call f3, passing the logger
    f3(log)

def f1(log):
    # We are in f1. Create a new log frame.
    log = log.new_frame("f1")
    
    log.debug(
        "F1_DATA_ENTRY",
        "Entering f1.",
        input_value=123
    )
    
    # Call f2, passing the logger
    f2(log)

def main_func():
    # Setup the base logger
    base_logger = setup_logger()
    
    # We are in main_func. Create the *first* log frame.
    log = CustomChainLogger(base_logger).new_frame("main_func")

    log.info(
        "MAIN_PROCESS_START", 
        "Starting main process...",
        config_file="prod.ini",
        user="admin"
    )
    
    # Call f1, passing the logger
    f1(log)

# --- Run the example ---
if __name__ == "__main__":
    main_func()

