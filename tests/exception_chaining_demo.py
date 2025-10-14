#!/usr/bin/env python3
# save as exception_chaining_demo.py and run: python exception_chaining_demo.py

import traceback

def raise_original():
    raise ValueError("original error (ValueError)")

def wrapped_with_from():
    try:
        raise_original()
    except Exception as e:
        # explicit chaining: new_exception.__cause__ == e
        raise RuntimeError("wrapped with 'from e' (RuntimeError)") from e

def wrapped_implicit():
    try:
        raise_original()
    except Exception:
        # implicit chaining: new_exception.__context__ holds the original
        raise RuntimeError("wrapped implicitly (no 'from')")

def wrapped_suppressed():
    try:
        raise_original()
    except Exception as e:
        # suppress chaining: __cause__ is None and __context__ is cleared for the new exception
        raise RuntimeError("wrapped with 'from None' (chaining suppressed)") from None

def run_and_show(fn):
    print("\n" + "="*60)
    print(f"Running: {fn.__name__}")
    try:
        fn()
    except Exception as ex:
        # full traceback printed. If cause/context exist they will be shown.
        traceback.print_exc()
        # show attributes for inspection
        print("\nEXCEPTION INSPECTION:")
        print("type:", type(ex).__name__)
        print("message:", ex)
        print(".__cause__ :", repr(ex.__cause__))
        print(".__context__:", repr(ex.__context__))
        print("="*60 + "\n")

if __name__ == "__main__":
    run_and_show(wrapped_with_from)
    run_and_show(wrapped_implicit)
    run_and_show(wrapped_suppressed)
