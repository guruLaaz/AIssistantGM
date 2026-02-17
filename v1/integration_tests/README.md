# Integration Tests

This folder contains integration tests for the Fantrax CLI application. These tests verify that the CLI commands work correctly end-to-end by running actual commands and validating their output.

## Test File

- **test_commands.py**: Integration tests for all CLI commands
  - Tests teams command with all output formats (table, JSON, simple)
  - Tests roster command with all output formats
  - Validates roster status (Active/Reserve/IR) display
  - Validates injury report (DTD/Out/IR/Suspended) display
  - Tests --last-n-days option for recent stats
  - Basic smoke tests for CLI help and error handling

## Prerequisites

Before running these tests, ensure:

1. You have installed the package: `pip install -e .`
2. You have configured your `.env` file with valid credentials
3. The `fantrax` command is available in your PATH

## Running the Tests

### Run all integration tests:

```bash
# From the project root
pytest integration_tests/

# With verbose output
pytest integration_tests/ -v

# With output from print statements
pytest integration_tests/ -s
```

### Run only integration tests (skip unit tests):

```bash
pytest -m integration
```

### Run all tests EXCEPT integration tests:

```bash
pytest -m "not integration"
```

### Run specific test classes or functions:

```bash
# Run only teams tests
pytest integration_tests/test_commands.py::TestTeamsCommand

# Run only roster tests
pytest integration_tests/test_commands.py::TestRosterCommand

# Run a specific test
pytest integration_tests/test_commands.py::TestRosterCommand::test_roster_status_column
```

### Run with coverage:

```bash
pytest integration_tests/ --cov=fantrax_cli --cov-report=html
```

## Test Organization

Tests are marked with `@pytest.mark.integration` to distinguish them from unit tests. This allows you to:

- Run only fast unit tests during development: `pytest -m "not integration"`
- Run integration tests before committing: `pytest -m integration`
- Run everything in CI/CD: `pytest`

## Test Structure

Tests use pytest fixtures and parametrization for clean, DRY code:

```python
@pytest.mark.integration
class TestRosterCommand:
    @pytest.mark.parametrize("format_arg,format_name", [
        ([], "table"),
        (["--format", "json"], "json"),
        (["--format", "simple"], "simple"),
    ])
    def test_roster_output_formats(self, cli_runner, format_arg, format_name):
        # Test implementation...
```

## Expected Output

Pytest provides clear test results:

```bash
$ pytest integration_tests/ -v

integration_tests/test_commands.py::TestTeamsCommand::test_teams_output_formats[table] PASSED
integration_tests/test_commands.py::TestTeamsCommand::test_teams_output_formats[json] PASSED
integration_tests/test_commands.py::TestTeamsCommand::test_teams_output_formats[simple] PASSED
integration_tests/test_commands.py::TestRosterCommand::test_roster_output_formats[table] PASSED
integration_tests/test_commands.py::TestRosterCommand::test_roster_output_formats[json] PASSED
integration_tests/test_commands.py::TestRosterCommand::test_roster_output_formats[simple] PASSED
integration_tests/test_commands.py::TestRosterCommand::test_roster_status_column PASSED
integration_tests/test_commands.py::TestRosterCommand::test_roster_status_values PASSED
integration_tests/test_commands.py::TestRosterCommand::test_roster_injury_report PASSED
integration_tests/test_commands.py::TestRosterCommand::test_roster_with_recent_stats PASSED
integration_tests/test_commands.py::TestCliBasics::test_cli_help PASSED
integration_tests/test_commands.py::TestCliBasics::test_invalid_command PASSED

========================= 12 passed in 45.23s =========================
```

## Notes

- These tests make real API calls to Fantrax, so they require authentication and network access
- Tests may take 30-60 seconds to complete due to rate limiting (1-second delay between requests)
- The `test_roster_with_recent_stats` test is particularly slow as it makes multiple API calls
- If tests fail, check your `.env` configuration and network connectivity
- Tests use a hardcoded team name "Bois ton (dro)let" - update in test_commands.py if testing against a different league

## CI/CD Integration

These tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run unit tests
  run: pytest -m "not integration"

- name: Run integration tests
  run: pytest -m integration
  env:
    FANTRAX_USERNAME: ${{ secrets.FANTRAX_USERNAME }}
    FANTRAX_PASSWORD: ${{ secrets.FANTRAX_PASSWORD }}
    FANTRAX_LEAGUE_ID: ${{ secrets.FANTRAX_LEAGUE_ID }}
```

## Adding New Tests

To add new integration tests:

1. Add test methods to existing test classes or create new classes in `test_commands.py`
2. Mark tests with `@pytest.mark.integration`
3. Use the `cli_runner` fixture to execute commands
4. Use pytest's assertion methods for validation
5. Run `pytest integration_tests/` to verify

Example:
```python
@pytest.mark.integration
class TestNewCommand:
    def test_new_feature(self, cli_runner):
        result = cli_runner("new-command", "--option", "value")
        assert result.returncode == 0
        assert "expected output" in result.stdout
```
