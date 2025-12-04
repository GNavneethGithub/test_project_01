# utils/time_utils.py

import re
from datetime import timedelta, datetime
from typing import Dict, Any
from utils.custom_logging import CustomChainLogger


def parse_duration_string(duration_str: str, logger: CustomChainLogger) -> Dict[str, Any]:
    """
    Parses a duration string and returns a timedelta object.
    
    The duration_str can contain any combination of (in strict order):
    - years (y)
    - months (mon)
    - weeks (w)
    - days (d)
    - hours (h)
    - minutes (m)
    - seconds (s)
    - milliseconds (ms)
    
    The components must appear in this order. Case-insensitive.
    
    Args:
        duration_str (str): Duration string like '1y2mon1w2d3h44m33s45ms' or '3h40m' or '0d'
        logger (CustomChainLogger): Custom logger for logging operations
    
    Returns:
        dict: A dictionary containing:
            - 'timedelta': timedelta object if successful, None otherwise
            - 'continue_dag_run': True if parsing successful, False otherwise
            - 'error_message': Error message if failed, None if successful
            - 'parsed_components': Dict of parsed components (for debugging)
    
    Example:
        >>> result = parse_duration_string('1y2mon1w2d3h44m33s45ms', logger)
        >>> if result['continue_dag_run']:
        ...     target_time = some_datetime + result['timedelta']
    """
    log_keyword = "PARSE_DURATION_STRING"
    log = logger.new_frame(log_keyword)
    
    return_object = {
        'timedelta': None,
        'continue_dag_run': False,
        'error_message': None,
        'parsed_components': {}
    }
    
    try:
        # Validate input
        if not isinstance(duration_str, str):
            error_msg = f"duration_str must be a string, got {type(duration_str).__name__}"
            log.error(
                log_keyword,
                error_msg,
                received_type=type(duration_str).__name__
            )
            return_object['error_message'] = error_msg
            return return_object
        
        duration_str = duration_str.strip()
        
        if not duration_str:
            error_msg = "duration_str cannot be empty"
            log.error(log_keyword, error_msg)
            return_object['error_message'] = error_msg
            return return_object
        
        log.info(
            log_keyword,
            "Starting to parse duration string.",
            input_duration=duration_str
        )
        
        # Convert to lowercase for case-insensitive matching
        duration_str_lower = duration_str.lower()
        
        # Dictionary to store parsed components
        # Order matters: years, months, weeks, days, hours, minutes, seconds, milliseconds
        components = {
            'years': 0,
            'months': 0,
            'weeks': 0,
            'days': 0,
            'hours': 0,
            'minutes': 0,
            'seconds': 0,
            'milliseconds': 0
        }
        
        # Define the expected order of units
        unit_order = ['y', 'mon', 'w', 'd', 'h', 'm', 's', 'ms']
        component_keys = ['years', 'months', 'weeks', 'days', 'hours', 'minutes', 'seconds', 'milliseconds']
        
        # Regular expression to match number + unit pairs (case-insensitive)
        pattern = r'(\d+)\s*(y|mon|w|d|h|m|s|ms)'
        
        matches = re.findall(pattern, duration_str_lower)
        
        if not matches:
            error_msg = f"No valid duration components found in '{duration_str}'. Expected format: 1y2mon1w2d3h44m33s45ms"
            log.error(
                log_keyword,
                error_msg,
                input_duration=duration_str
            )
            return_object['error_message'] = error_msg
            return return_object
        
        # Track the last seen unit index to enforce order
        last_unit_index = -1
        
        # Process each matched component
        for value_str, unit in matches:
            value = int(value_str)
            
            # Find the unit in our expected order
            if unit not in unit_order:
                error_msg = f"Unknown unit '{unit}' in duration string"
                log.error(
                    log_keyword,
                    error_msg,
                    unknown_unit=unit,
                    input_duration=duration_str
                )
                return_object['error_message'] = error_msg
                return return_object
            
            current_unit_index = unit_order.index(unit)
            
            # Check if units are in the correct order
            if current_unit_index <= last_unit_index:
                error_msg = f"Duration components must be in order (year, month, week, day, hour, minute, second, millisecond). Found '{unit}' after '{unit_order[last_unit_index]}'"
                log.error(
                    log_keyword,
                    error_msg,
                    current_unit=unit,
                    previous_unit=unit_order[last_unit_index],
                    input_duration=duration_str
                )
                return_object['error_message'] = error_msg
                return return_object
            
            last_unit_index = current_unit_index
            
            # Store the component
            component_key = component_keys[current_unit_index]
            components[component_key] = value
            log.debug(log_keyword, f"Parsed {component_key}.", value=value)
        
        # Validate that all characters in input were consumed
        cleaned_input = re.sub(r'[\d\s]', '', duration_str_lower)
        cleaned_input = re.sub(r'[ymonwdhms]', '', cleaned_input)
        if cleaned_input:
            error_msg = f"Invalid characters in duration string: '{cleaned_input}'. Expected only: y, mon, w, d, h, m, s, ms"
            log.error(
                log_keyword,
                error_msg,
                input_duration=duration_str,
                invalid_chars=cleaned_input
            )
            return_object['error_message'] = error_msg
            return return_object
        
        # Create timedelta object
        # Note: timedelta doesn't support years and months directly
        # We approximate: 1 year = 365 days, 1 month = 30 days
        days_from_years = components['years'] * 365
        days_from_months = components['months'] * 30
        total_days = days_from_years + days_from_months + components['days']
        
        td = timedelta(
            weeks=components['weeks'],
            days=total_days,
            hours=components['hours'],
            minutes=components['minutes'],
            seconds=components['seconds'],
            milliseconds=components['milliseconds']
        )
        
        log.info(
            log_keyword,
            "Successfully parsed duration string.",
            input_duration=duration_str,
            total_seconds=td.total_seconds(),
            components=components,
            approximation_note="Years (365 days) and Months (30 days) are approximations"
        )
        
        return_object['timedelta'] = td
        return_object['continue_dag_run'] = True
        return_object['parsed_components'] = components
        
        return return_object
    
    except ValueError as e:
        # Handles cases where int() conversion fails
        error_msg = f"Invalid numeric value in duration string: {str(e)}"
        log.error(
            log_keyword,
            error_msg,
            exc_info=True,
            input_duration=duration_str
        )
        return_object['error_message'] = error_msg
        return return_object
    
    except Exception as e:
        # Catch-all for unexpected errors
        error_msg = f"Unexpected error while parsing duration string: {str(e)}"
        log.error(
            log_keyword,
            error_msg,
            exc_info=True,
            input_duration=duration_str,
            error_type=type(e).__name__
        )
        return_object['error_message'] = error_msg
        return return_object


def timedelta_to_duration_string(tmax, tmin, logger: CustomChainLogger) -> Dict[str, Any]:
    """
    Converts the difference between two datetime objects to a duration string.
    
    Calculates tmax - tmin and converts the resulting timedelta to a duration string
    in the format: 1y2mon1w2d3h44m33s45ms
    
    Only non-zero components are included in the output.
    If the result is negative, a '-' prefix is added.
    
    Args:
        tmax: datetime object (any datetime, no size check performed)
        tmin: datetime object (any datetime, no size check performed)
        logger (CustomChainLogger): Custom logger for logging operations
    
    Returns:
        dict: A dictionary containing:
            - 'duration_string': Duration string if successful, None otherwise
            - 'continue_dag_run': True if conversion successful, False otherwise
            - 'error_message': Error message if failed, None if successful
            - 'timedelta': The calculated timedelta object
            - 'is_negative': True if duration is negative, False otherwise
            - 'components': Dict of calculated components (for debugging)
    
    Example:
        >>> from datetime import datetime
        >>> t1 = datetime(2024, 1, 1)
        >>> t2 = datetime(2024, 1, 5, 3, 30, 45, 500000)  # 4 days, 3 hours, 30 mins, 45.5 secs
        >>> result = timedelta_to_duration_string(t2, t1, logger)
        >>> # result['duration_string'] = '4d3h30m45s500ms'
    """
    log_keyword = "TIMEDELTA_TO_DURATION_STRING"
    log = logger.new_frame(log_keyword)
    
    return_object = {
        'duration_string': None,
        'continue_dag_run': False,
        'error_message': None,
        'timedelta': None,
        'is_negative': False,
        'components': {}
    }
    
    try:
        # Validate inputs
        if not isinstance(tmax, datetime):
            error_msg = f"tmax must be a datetime object, got {type(tmax).__name__}"
            log.error(
                log_keyword,
                error_msg,
                received_type=type(tmax).__name__
            )
            return_object['error_message'] = error_msg
            return return_object
        
        if not isinstance(tmin, datetime):
            error_msg = f"tmin must be a datetime object, got {type(tmin).__name__}"
            log.error(
                log_keyword,
                error_msg,
                received_type=type(tmin).__name__
            )
            return_object['error_message'] = error_msg
            return return_object
        
        log.info(
            log_keyword,
            "Starting to convert timedelta to duration string.",
            tmax=tmax,
            tmin=tmin
        )
        
        # Calculate the difference
        td = tmax - tmin
        
        # Check if negative
        is_negative = td.total_seconds() < 0
        
        # Work with absolute value for calculations
        if is_negative:
            td = abs(td)
        
        log.debug(
            log_keyword,
            "Timedelta calculated.",
            total_seconds=td.total_seconds(),
            is_negative=is_negative
        )
        
        # Extract components from timedelta
        total_seconds = td.total_seconds()
        
        # Calculate each component
        components = {
            'years': 0,
            'months': 0,
            'weeks': 0,
            'days': 0,
            'hours': 0,
            'minutes': 0,
            'seconds': 0,
            'milliseconds': 0
        }
        
        # Convert seconds to various units
        # 1 year = 365 days = 31,536,000 seconds
        # 1 month = 30 days = 2,592,000 seconds
        # 1 week = 7 days = 604,800 seconds
        # 1 day = 86,400 seconds
        # 1 hour = 3,600 seconds
        # 1 minute = 60 seconds
        # 1 millisecond = 0.001 seconds
        
        remaining_seconds = total_seconds
        
        # Years (365 days)
        seconds_per_year = 365 * 24 * 60 * 60
        components['years'] = int(remaining_seconds // seconds_per_year)
        remaining_seconds -= components['years'] * seconds_per_year
        
        # Months (30 days)
        seconds_per_month = 30 * 24 * 60 * 60
        components['months'] = int(remaining_seconds // seconds_per_month)
        remaining_seconds -= components['months'] * seconds_per_month
        
        # Weeks (7 days)
        seconds_per_week = 7 * 24 * 60 * 60
        components['weeks'] = int(remaining_seconds // seconds_per_week)
        remaining_seconds -= components['weeks'] * seconds_per_week
        
        # Days
        seconds_per_day = 24 * 60 * 60
        components['days'] = int(remaining_seconds // seconds_per_day)
        remaining_seconds -= components['days'] * seconds_per_day
        
        # Hours
        seconds_per_hour = 60 * 60
        components['hours'] = int(remaining_seconds // seconds_per_hour)
        remaining_seconds -= components['hours'] * seconds_per_hour
        
        # Minutes
        seconds_per_minute = 60
        components['minutes'] = int(remaining_seconds // seconds_per_minute)
        remaining_seconds -= components['minutes'] * seconds_per_minute
        
        # Seconds and milliseconds
        components['seconds'] = int(remaining_seconds)
        remaining_milliseconds = (remaining_seconds - components['seconds']) * 1000
        components['milliseconds'] = int(round(remaining_milliseconds))
        
        log.debug(
            log_keyword,
            "Components extracted from timedelta.",
            components=components
        )
        
        # Build the duration string (only non-zero components)
        duration_parts = []
        
        if components['years'] > 0:
            duration_parts.append(f"{components['years']}y")
        if components['months'] > 0:
            duration_parts.append(f"{components['months']}mon")
        if components['weeks'] > 0:
            duration_parts.append(f"{components['weeks']}w")
        if components['days'] > 0:
            duration_parts.append(f"{components['days']}d")
        if components['hours'] > 0:
            duration_parts.append(f"{components['hours']}h")
        if components['minutes'] > 0:
            duration_parts.append(f"{components['minutes']}m")
        if components['seconds'] > 0:
            duration_parts.append(f"{components['seconds']}s")
        if components['milliseconds'] > 0:
            duration_parts.append(f"{components['milliseconds']}ms")
        
        # Handle the case where all components are zero
        if not duration_parts:
            duration_string = "0s"
        else:
            duration_string = "".join(duration_parts)
        
        # Add negative sign if needed
        if is_negative:
            duration_string = "-" + duration_string
        
        log.info(
            log_keyword,
            "Successfully converted timedelta to duration string.",
            duration_string=duration_string,
            is_negative=is_negative,
            components=components,
            total_seconds=td.total_seconds()
        )
        
        return_object['duration_string'] = duration_string
        return_object['continue_dag_run'] = True
        return_object['timedelta'] = td if not is_negative else -td
        return_object['is_negative'] = is_negative
        return_object['components'] = components
        
        return return_object
    
    except Exception as e:
        error_msg = f"Unexpected error while converting timedelta to duration string: {str(e)}"
        log.error(
            log_keyword,
            error_msg,
            exc_info=True,
            error_type=type(e).__name__
        )
        return_object['error_message'] = error_msg
        return return_object


def iso_timestamp_to_datetime(iso_timestamp_str: str, logger: CustomChainLogger) -> Dict[str, Any]:
    """
    Converts an ISO 8601 standard timestamp string with timezone info to a timezone-aware datetime object.
    
    Accepts ISO 8601 format timestamps with timezone information:
    - 2024-01-15T10:30:45Z (UTC with Z notation)
    - 2024-01-15T10:30:45+00:00 (UTC with offset notation)
    - 2024-01-15T10:30:45-05:00 (with timezone offset)
    - 2024-01-15T10:30:45.500+05:30 (with milliseconds and timezone)
    - 2024-01-15T10:30:45.123456-08:00 (with microseconds and timezone)
    
    The returned datetime object is timezone-aware (preserves timezone information).
    
    Args:
        iso_timestamp_str (str): ISO 8601 standard timestamp string with timezone info
        logger (CustomChainLogger): Custom logger for logging operations
    
    Returns:
        dict: A dictionary containing:
            - 'datetime': datetime object (timezone-aware) if successful, None otherwise
            - 'continue_dag_run': True if conversion successful, False otherwise
            - 'error_message': Error message if failed, None if successful
            - 'timezone_info': Timezone information (str) for debugging
    
    Example:
        >>> result = iso_timestamp_to_datetime('2024-01-15T10:30:45+05:30', logger)
        >>> if result['continue_dag_run']:
        ...     dt = result['datetime']
        ...     print(dt)  # 2024-01-15 10:30:45+05:30
    """
    log_keyword = "ISO_TIMESTAMP_TO_DATETIME"
    log = logger.new_frame(log_keyword)
    
    return_object = {
        'datetime': None,
        'continue_dag_run': False,
        'error_message': None,
        'timezone_info': None
    }
    
    try:
        # Validate input
        if not isinstance(iso_timestamp_str, str):
            error_msg = f"iso_timestamp_str must be a string, got {type(iso_timestamp_str).__name__}"
            log.error(
                log_keyword,
                error_msg,
                received_type=type(iso_timestamp_str).__name__,
                user_input=iso_timestamp_str
            )
            return_object['error_message'] = error_msg
            return return_object
        
        iso_timestamp_str = iso_timestamp_str.strip()
        
        if not iso_timestamp_str:
            error_msg = "iso_timestamp_str cannot be empty"
            log.error(log_keyword, error_msg, user_input=iso_timestamp_str)
            return_object['error_message'] = error_msg
            return return_object
        
        log.info(
            log_keyword,
            "Starting to convert ISO timestamp string to datetime object.",
            user_input=iso_timestamp_str
        )
        
        # Try to parse the ISO timestamp string
        # Python's fromisoformat() method handles most ISO 8601 formats
        # But it doesn't handle 'Z' notation, so we need to replace it with '+00:00'
        timestamp_normalized = iso_timestamp_str
        
        if timestamp_normalized.endswith('Z'):
            timestamp_normalized = timestamp_normalized[:-1] + '+00:00'
            log.debug(
                log_keyword,
                "Normalized 'Z' notation to '+00:00'.",
                original_input=iso_timestamp_str,
                normalized_input=timestamp_normalized
            )
        
        # Parse the timestamp using fromisoformat
        try:
            dt = datetime.fromisoformat(timestamp_normalized)
        except ValueError as e:
            # If fromisoformat fails, try manual parsing for edge cases
            error_msg = f"Invalid ISO 8601 timestamp format: {str(e)}"
            log.error(
                log_keyword,
                error_msg,
                user_input=iso_timestamp_str,
                error_details=str(e)
            )
            return_object['error_message'] = error_msg
            return return_object
        
        # Verify that the datetime object is timezone-aware
        if dt.tzinfo is None:
            error_msg = "Parsed datetime is not timezone-aware. Input must include timezone information (Z or ±HH:MM)"
            log.error(
                log_keyword,
                error_msg,
                user_input=iso_timestamp_str
            )
            return_object['error_message'] = error_msg
            return return_object
        
        # Extract timezone information
        timezone_info = str(dt.tzinfo)
        
        log.info(
            log_keyword,
            "Successfully converted ISO timestamp string to datetime object.",
            user_input=iso_timestamp_str,
            parsed_datetime=str(dt),
            timezone_info=timezone_info
        )
        
        return_object['datetime'] = dt
        return_object['continue_dag_run'] = True
        return_object['timezone_info'] = timezone_info
        
        return return_object
    
    except TypeError as e:
        error_msg = f"Type error while parsing ISO timestamp: {str(e)}"
        log.error(
            log_keyword,
            error_msg,
            exc_info=True,
            user_input=iso_timestamp_str,
            error_type=type(e).__name__
        )
        return_object['error_message'] = error_msg
        return return_object
    
    except Exception as e:
        # Catch-all for unexpected errors
        error_msg = f"Unexpected error while converting ISO timestamp string: {str(e)}"
        log.error(
            log_keyword,
            error_msg,
            exc_info=True,
            user_input=iso_timestamp_str,
            error_type=type(e).__name__
        )
        return_object['error_message'] = error_msg
        return return_object



def extract_datetime_components(dt: datetime, logger: CustomChainLogger) -> Dict[str, Any]:
    """
    Extracts date and time strings from a datetime object.
    
    Extracts:
    - Date in YYYY-MM-DD format
    - Time in HH-MM-SS format (milliseconds/microseconds are ignored)
    - Time in HH_MM format
    - date object from datetime
    - datetime object (original input)
    
    Args:
        dt (datetime): datetime object (can be timezone-aware or naive)
        logger (CustomChainLogger): Custom logger for logging operations
    
    Returns:
        dict: A dictionary containing:
            - 'continue_dag_run': True if extraction successful, False otherwise
            - 'error_message': Error message if failed, None if successful
            - 'date_str': Date in YYYY-MM-DD format (e.g., '2024-01-15')
            - 'time_str': Time in HH-MM-SS format (e.g., '10-30-45')
            - 'hhmm_str': Time in HH_MM format (e.g., '10_30')
            - 'date_obj': date object (e.g., date(2024, 1, 15))
            - 'datetime_obj': datetime object (original input)
    
    Example:
        >>> from datetime import datetime
        >>> dt = datetime(2024, 1, 15, 10, 30, 45, 500000)
        >>> result = extract_datetime_components(dt, logger)
        >>> if result['continue_dag_run']:
        ...     print(result['date_str'])     # 2024-01-15
        ...     print(result['time_str'])     # 10-30-45
        ...     print(result['hhmm_str'])     # 10_30
        ...     print(result['date_obj'])     # 2024-01-15
        ...     print(result['datetime_obj']) # 2024-01-15 10:30:45.500000
    """
    log_keyword = "EXTRACT_DATETIME_COMPONENTS"
    log = logger.new_frame(log_keyword)
    
    return_object = {
        'continue_dag_run': False,
        'error_message': None,
        'date_str': None,
        'time_str': None,
        'hhmm_str': None,
        'date_obj': None,
        'datetime_obj': None
    }
    
    try:
        # Validate input
        if not isinstance(dt, datetime):
            error_msg = f"dt must be a datetime object, got {type(dt).__name__}"
            log.error(
                log_keyword,
                error_msg,
                received_type=type(dt).__name__,
                input_value=str(dt)
            )
            return_object['error_message'] = error_msg
            return return_object
        
        log.info(
            log_keyword,
            "Starting to extract datetime components.",
            input_datetime=str(dt),
            has_timezone=dt.tzinfo is not None
        )
        
        # Extract date components
        year = dt.year
        month = dt.month
        day = dt.day
        
        # Extract time components (ignoring microseconds/milliseconds)
        hour = dt.hour
        minute = dt.minute
        second = dt.second
        
        # Format date string in YYYY-MM-DD format
        date_str = f"{year:04d}-{month:02d}-{day:02d}"
        
        # Format time string in HH-MM-SS format
        time_str = f"{hour:02d}-{minute:02d}-{second:02d}"
        
        # Format time string in HH_MM format
        hhmm_str = f"{hour:02d}_{minute:02d}"
        
        # Get date object from datetime
        date_obj = dt.date()
        
        log.debug(
            log_keyword,
            "Components extracted successfully.",
            date_str=date_str,
            time_str=time_str,
            hhmm_str=hhmm_str,
            date_obj=str(date_obj)
        )
        
        log.info(
            log_keyword,
            "Successfully extracted datetime components.",
            input_datetime=str(dt),
            date_str=date_str,
            time_str=time_str,
            hhmm_str=hhmm_str,
            date_obj=str(date_obj)
        )
        
        return_object['continue_dag_run'] = True
        return_object['date_str'] = date_str
        return_object['time_str'] = time_str
        return_object['hhmm_str'] = hhmm_str
        return_object['date_obj'] = date_obj
        return_object['datetime_obj'] = dt
        
        return return_object
    
    except AttributeError as e:
        error_msg = f"AttributeError while extracting components: {str(e)}"
        log.error(
            log_keyword,
            error_msg,
            exc_info=True,
            input_value=str(dt),
            error_type=type(e).__name__
        )
        return_object['error_message'] = error_msg
        return return_object
    
    except Exception as e:
        # Catch-all for unexpected errors
        error_msg = f"Unexpected error while extracting datetime components: {str(e)}"
        log.error(
            log_keyword,
            error_msg,
            exc_info=True,
            input_value=str(dt),
            error_type=type(e).__name__
        )
        return_object['error_message'] = error_msg
        return return_object



def get_timestamp_floor(dt: datetime, duration_str: str, logger: CustomChainLogger) -> Dict[str, Any]:
    """
    Finds the floor value of a timestamp based on duration intervals.
    
    Divides a day into intervals of the given duration and finds which interval 
    the timestamp falls into. Returns the lower bound (start time) of that interval.
    
    The timezone of the original datetime is preserved.
    
    Args:
        dt (datetime): datetime object (can be timezone-aware or naive)
        duration_str (str): Duration string like '2h', '30m', '1d', '15m', etc.
                           Must be a valid duration format (see parse_duration_string)
        logger (CustomChainLogger): Custom logger for logging operations
    
    Returns:
        dict: A dictionary containing:
            - 'continue_dag_run': True if calculation successful, False otherwise
            - 'error_message': Error message if failed, None if successful
            - 'floor_datetime': Floor datetime object (timezone-aware) if successful, None otherwise
            - 'interval_seconds': Duration of the interval in seconds
            - 'intervals_passed': Number of complete intervals passed since midnight
    
    Example:
        >>> from datetime import datetime
        >>> dt = datetime(2025, 11, 27, 5, 33, 11)  # timezone-aware or naive
        >>> result = get_timestamp_floor(dt, '2h', logger)
        >>> if result['continue_dag_run']:
        ...     print(result['floor_datetime'])  # 2025-11-27 04:00:00
        
        >>> result = get_timestamp_floor(dt, '30m', logger)
        >>> if result['continue_dag_run']:
        ...     print(result['floor_datetime'])  # 2025-11-27 05:30:00
        
        >>> result = get_timestamp_floor(dt, '1d', logger)
        >>> if result['continue_dag_run']:
        ...     print(result['floor_datetime'])  # 2025-11-27 00:00:00
    """
    log_keyword = "GET_TIMESTAMP_FLOOR"
    log = logger.new_frame(log_keyword)
    
    return_object = {
        'continue_dag_run': False,
        'error_message': None,
        'floor_datetime': None,
        'interval_seconds': None,
        'intervals_passed': None
    }
    
    try:
        # Validate inputs
        if not isinstance(dt, datetime):
            error_msg = f"dt must be a datetime object, got {type(dt).__name__}"
            log.error(
                log_keyword,
                error_msg,
                received_type=type(dt).__name__,
                input_datetime=str(dt)
            )
            return_object['error_message'] = error_msg
            return return_object
        
        if not isinstance(duration_str, str):
            error_msg = f"duration_str must be a string, got {type(duration_str).__name__}"
            log.error(
                log_keyword,
                error_msg,
                received_type=type(duration_str).__name__,
                duration_input=str(duration_str)
            )
            return_object['error_message'] = error_msg
            return return_object
        
        log.info(
            log_keyword,
            "Starting to calculate timestamp floor.",
            input_datetime=str(dt),
            duration_str=duration_str,
            has_timezone=dt.tzinfo is not None
        )
        
        # Import parse_duration_string from this module (or it will be imported from time_utils)
        # For now, we'll parse the duration manually using a simplified approach
        # Parse duration string to get total seconds
        duration_seconds = _parse_duration_to_seconds(duration_str, log)
        
        if duration_seconds is None:
            error_msg = f"Invalid duration string: '{duration_str}'. Expected format like '2h', '30m', '1d', '15m', etc."
            log.error(
                log_keyword,
                error_msg,
                duration_input=duration_str
            )
            return_object['error_message'] = error_msg
            return return_object
        
        # Validate that duration_seconds is positive
        if duration_seconds <= 0:
            error_msg = f"Duration must be positive, got {duration_seconds} seconds"
            log.error(
                log_keyword,
                error_msg,
                duration_seconds=duration_seconds
            )
            return_object['error_message'] = error_msg
            return return_object
        
        # Get seconds since midnight for the given datetime
        seconds_since_midnight = dt.hour * 3600 + dt.minute * 60 + dt.second + (dt.microsecond / 1_000_000)
        
        log.debug(
            log_keyword,
            "Calculated seconds since midnight.",
            seconds_since_midnight=seconds_since_midnight,
            duration_seconds=duration_seconds
        )
        
        # Calculate how many complete intervals have passed since midnight
        intervals_passed = int(seconds_since_midnight // duration_seconds)
        
        # Calculate the start time of the current interval (in seconds since midnight)
        floor_seconds_since_midnight = intervals_passed * duration_seconds
        
        # Convert back to hours, minutes, seconds
        floor_hours = int(floor_seconds_since_midnight // 3600)
        floor_minutes = int((floor_seconds_since_midnight % 3600) // 60)
        floor_seconds = int(floor_seconds_since_midnight % 60)
        
        # Create a new datetime with the floor time, preserving the date and timezone
        floor_datetime = dt.replace(
            hour=floor_hours,
            minute=floor_minutes,
            second=floor_seconds,
            microsecond=0
        )
        
        log.debug(
            log_keyword,
            "Calculated floor datetime.",
            floor_datetime=str(floor_datetime),
            intervals_passed=intervals_passed,
            floor_time_components=f"H:{floor_hours}, M:{floor_minutes}, S:{floor_seconds}"
        )
        
        log.info(
            log_keyword,
            "Successfully calculated timestamp floor.",
            input_datetime=str(dt),
            duration_str=duration_str,
            floor_datetime=str(floor_datetime),
            intervals_passed=intervals_passed,
            interval_seconds=duration_seconds
        )
        
        return_object['continue_dag_run'] = True
        return_object['floor_datetime'] = floor_datetime
        return_object['interval_seconds'] = duration_seconds
        return_object['intervals_passed'] = intervals_passed
        
        return return_object
    
    except Exception as e:
        error_msg = f"Unexpected error while calculating timestamp floor: {str(e)}"
        log.error(
            log_keyword,
            error_msg,
            exc_info=True,
            input_datetime=str(dt),
            duration_input=duration_str,
            error_type=type(e).__name__
        )
        return_object['error_message'] = error_msg
        return return_object


def _parse_duration_to_seconds(duration_str: str, log) -> int:
    """
    Helper function to parse simple duration strings to seconds.
    Handles: s, m, h, d (and combinations like 2h30m)
    
    Returns:
        int: Total seconds, or None if invalid format
    """
    
    duration_str = duration_str.lower().strip()
    
    # Pattern to match number + unit pairs
    pattern = r'(\d+)\s*([smhd])'
    matches = re.findall(pattern, duration_str)
    
    if not matches:
        return None
    
    # Conversion factors
    unit_to_seconds = {
        's': 1,
        'm': 60,
        'h': 3600,
        'd': 86400
    }
    
    total_seconds = 0
    
    for value_str, unit in matches:
        value = int(value_str)
        if unit not in unit_to_seconds:
            return None
        
        total_seconds += value * unit_to_seconds[unit]
    
    return total_seconds if total_seconds > 0 else None



def convert_timedelta_to_duration_string(
    delta_t: timedelta,
    logger: CustomChainLogger,
) -> Dict[str, Any]:
    """
    Convert a timedelta object into a compact duration string.
    Reverse of parse_duration_string().

    Example:
        timedelta(days=1, hours=2, minutes=30, seconds=15, milliseconds=500)
        -> "1d2h30m15s500ms"

    Returns:
        {
            "continue_dag_run": bool,
            "error_message": Optional[str],
            "duration_string": Optional[str],
        }
    """
    log_keyword = "CONVERT_TIMEDELTA_TO_DURATION_STRING"
    log = logger.new_frame(log_keyword)

    result = {
        "continue_dag_run": True,
        "error_message": None,
        "duration_string": None,
    }

    try:
        if delta_t is None:
            error_message = (
                f"[{log_keyword}] Input timedelta object cannot be None."
            )
            result["continue_dag_run"] = False
            result["error_message"] = error_message
            log.error(log_keyword, error_message)
            return result

        total_ms = int(delta_t.total_seconds() * 1000)

        # Extract components
        days = total_ms // (24 * 60 * 60 * 1000)
        total_ms %= (24 * 60 * 60 * 1000)

        hours = total_ms // (60 * 60 * 1000)
        total_ms %= (60 * 60 * 1000)

        minutes = total_ms // (60 * 1000)
        total_ms %= (60 * 1000)

        seconds = total_ms // 1000
        total_ms %= 1000

        milliseconds = total_ms

        # Build duration string
        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if seconds > 0:
            parts.append(f"{seconds}s")
        if milliseconds > 0:
            parts.append(f"{milliseconds}ms")

        # If timedelta was exactly zero
        if not parts:
            parts.append("0s")

        duration_string = "".join(parts)
        result["duration_string"] = duration_string

        log.info(
            log_keyword,
            "Converted timedelta to duration string successfully",
            duration_string=duration_string,
        )

        return result

    except Exception as e:
        error_message = (
            f"[{log_keyword}] Unexpected error while converting timedelta to duration string: {e}"
        )
        result["continue_dag_run"] = False
        result["error_message"] = error_message

        log.error(
            log_keyword,
            "Unexpected exception",
            exception=str(e),
            error_message=error_message,
        )

        return result







