---
applyTo: "**/*.py"
---

# Python Code Instructions

- Use Python 3.11+ features (type hints, match statements where appropriate)
- All functions must have Google-style docstrings with Args, Returns, Raises sections
- Use Pydantic v2 for data validation and schema definitions
- Use `pathlib.Path` instead of `os.path`
- Use `logging` module, never `print()` in production code (print is OK in scripts/)
- Handle exceptions explicitly — no bare `except:` clauses
- All config values must come from environment variables via `src/config/settings.py`
- Never hardcode API keys, bucket names, or connection strings
- Use `typing` module for complex type hints (Optional, Union, List, Dict)
- Prefer list comprehensions over map/filter for readability
- Use context managers (`with` statements) for file and connection handling