# Fantrax CLI

A Python command-line interface for the Fantrax Fantasy Sports API.

## Features

- Retrieve and display team information from your Fantrax league
- Support for private leagues with automatic authentication
- Multiple output formats (table, JSON, simple text)
- Cookie caching to minimize repeated logins

## Installation

### Prerequisites

- Python 3.11 or higher (required by FantraxAPI)
- Chrome browser (required for authentication)

### Setup

1. Clone this repository or download the source code

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Configure your credentials:
```bash
cp .env.example .env
```

Edit the `.env` file with your Fantrax credentials:
```
FANTRAX_USERNAME=your_email@example.com
FANTRAX_PASSWORD=your_password
FANTRAX_LEAGUE_ID=88nib84gmbc80pu3
```

4. Install the package and dependencies:
```bash
pip install -e .
```

**Note for Windows:** If `pip` is not recognized, use:
```bash
python -m pip install -e .
```

This will install all dependencies listed in pyproject.toml and make the `fantrax` command available.

## Usage

### View Teams

Display all teams in your league:

```bash
# Table format (default)
fantrax teams

# JSON format
fantrax teams --format json

# Simple text format
fantrax teams --format simple
```

### Command Line Options

```bash
# Override league ID
fantrax --league-id YOUR_LEAGUE_ID teams

# Show version
fantrax --version

# Show help
fantrax --help
fantrax teams --help
```

### Running as a Module

You can also run the CLI as a Python module:

```bash
python -m fantrax_cli teams
```

## Authentication

For private leagues, the CLI uses Selenium to automate browser login:

1. On first run, Chrome browser will open in headless mode
2. The CLI will automatically log in using your credentials
3. Session cookies are saved to `fantraxloggedin.cookie`
4. Subsequent runs will reuse cached cookies (no browser launch needed)

If authentication fails, delete the cookie file and try again:
```bash
rm fantraxloggedin.cookie  # On Windows: del fantraxloggedin.cookie
```

## Project Structure

```
AIssistantGM/
├── fantrax_cli/
│   ├── __init__.py          # Package initialization
│   ├── __main__.py          # Entry point for module execution
│   ├── cli.py               # Main CLI interface
│   ├── auth.py              # Selenium authentication
│   ├── config.py            # Configuration management
│   ├── display.py           # Output formatting
│   └── commands/
│       ├── __init__.py
│       └── teams.py         # Teams command (with monkey-patch for scoringPeriodList bug)
├── fantraxapi_fork/         # Modified fork of FantraxAPI (for reference)
├── tests/                   # Test suite
├── .env                     # Your credentials (not committed)
├── .env.example             # Template for credentials
├── .gitignore
├── README.md
└── pyproject.toml           # Package config and dependencies
```

## Development

### Running Tests

```bash
pytest tests/
```

### Adding New Commands

1. Create a new file in `fantrax_cli/commands/`
2. Implement your command function
3. Register it in `fantrax_cli/cli.py`

Example:
```python
# In cli.py
from fantrax_cli.commands.your_command import your_command
app.command("your-command")(your_command)
```

## Known Issues & Solutions

### FantraxAPI scoringPeriodList Bug
Some leagues don't have a `scoringPeriodList` field in the API response, causing the original FantraxAPI library to crash. This CLI includes a monkey-patch in [teams.py](fantrax_cli/commands/teams.py) that handles missing `scoringPeriodList` gracefully. The fix is also applied to the forked version in `fantraxapi_fork/`.

## Troubleshooting

### "Missing required environment variables"
Make sure you've created a `.env` file with all required variables. See `.env.example` for reference.

### "NotLoggedIn" or authentication errors
Delete the cookie file and try again:
```bash
rm fantraxloggedin.cookie  # On Windows: del fantraxloggedin.cookie
```

### Chrome driver issues
The `webdriver-manager` package should automatically download the correct Chrome driver. If you encounter issues, ensure Chrome browser is installed and up to date.

## License

This project is provided as-is for personal use.

## Credits

Built using:
- [FantraxAPI](https://github.com/meisnate12/FantraxAPI) by meisnate12
- [Typer](https://typer.tiangolo.com/) for CLI framework
- [Rich](https://rich.readthedocs.io/) for beautiful terminal output
