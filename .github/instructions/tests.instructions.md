---
applyTo: "tests/**/*.py"
---

# Testing Instructions

- Follow TDD: write the failing test FIRST, then implement the code
- Unit tests must be fast, pure, and hermetic — no network, filesystem, or external services
- Use `pytest` as the test framework with `pytest-cov` for coverage
- Use `unittest.mock.patch` or `pytest-mock` to mock external dependencies (API calls, Kafka, S3, Spark)
- Minimum 80% code coverage on all new/changed files
- Test file naming: `test_{module_name}.py` matching the source module
- Use `conftest.py` for shared fixtures (mock Spark sessions, mock S3 clients, sample data)
- Test happy paths, edge cases, AND error handling for every function
- Use `@pytest.fixture` for test data setup
- Use `@pytest.mark.parametrize` for testing multiple input variations
- Never make real API calls in unit tests — always mock `requests.get`, `yfinance.download`, etc.
- Integration tests go in `tests/integration/` and may use Docker services