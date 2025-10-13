# test_logging_levels.py
"""
Run this next to your logging_util.py.
Shows: numeric levels, convenience methods, typo fallback, and level filtering.
Usage: python test_logging_levels.py
"""

import logging
from logging_util import get_logger

def show_level_mapping(level_input):
    return logging.getLevelName(level_input if isinstance(level_input, str) else int(level_input))

def main():
    # logger at DEBUG so all messages appear initially
    logger = get_logger("level_test", level="DEBUG")

    print("\n=== Run 1: logger level = DEBUG (all messages should appear) ===\n")

    # numeric constants via convenience wrappers
    logger.debug_ctx("numeric DEBUG message", user_message="num_debug", extra_data={"phase": 1})
    logger.info_ctx("numeric INFO message", user_message="num_info", extra_data={"phase": 1})
    logger.warning_ctx("numeric WARNING message", user_message="num_warn", extra_data={"phase": 1})
    logger.error_ctx("numeric ERROR message", user_message="num_error", extra_data={"phase": 1})
    logger.critical_ctx("numeric CRITICAL message", user_message="num_crit", extra_data={"phase": 1})

    print("=" * 60)
    # string-name variants (case-insensitive). Use convenience wrappers again.
    logger.debug_ctx("string DEBUG message", user_message="str_debug", extra_data={"phase": 2})
    logger.info_ctx("string info message lower-case", user_message="str_info", extra_data={"phase": 2})
    logger.warning_ctx("string Warning message mixed-case", user_message="str_warn", extra_data={"phase": 2})

    # typo case: "WARNNING" (intentional misspelling)
    typo = "WARNNING"
    mapped = show_level_mapping(typo.upper())
    logger.info_ctx(f"Mapping for '{typo.upper()}' -> {mapped} (helper fallback applies).",
                    user_message="level_map", extra_data={"input": typo, "mapped": str(mapped)})
    # call with the misspelled string (log_with_ctx handles fallback to INFO)
    logger.log_with_ctx(typo, "this used 'WARNNING' (typo) as level; check mapping above",
                        user_message="typo_level", extra_data={"phase": 3})

    print("\n=== Run 2: raise logger level to INFO (DEBUG should be suppressed) ===\n")

    # set underlying logger level to INFO to demonstrate filtering
    logging.getLogger("level_test").setLevel(logging.INFO)

    # debug should be suppressed now
    logger.debug_ctx("this DEBUG should NOT appear after level set to INFO", user_message="filtered_debug")
    logger.info_ctx("this INFO should appear", user_message="still_info")
    logger.error_ctx("this ERROR should appear", user_message="still_error")

    print("\n=== Done ===\n")

if __name__ == "__main__":
    main()
