`refacsql` is a command-line tool for AI-assisted refactoring of legacy T-SQL stored procedures. It performs dependency analysis, and maintains a detailed audit of all exchanges.

## Features
- Refactor large legacy stored procedures using AI
- Auto-detect and request missing table/function/type metadata
- Recursively resolve dependencies (configurable depth)
- Log all interactions and decisions in a JSONL audit file

## Install
```bash
pip install -r requirements.txt
```

Install ODBC drivers on Mac
```bash
# Tap the Microsoft repository (if you haven't already)
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release

# Update brew and install the driver and command-line tools
brew update
brew install msodbcsql18 mssql-tools18
```

## Usage
```bash
python refactor_proc.py \
  --proc-name usp_ProcessOrders \
  --audit-log ./logs/usp_ProcessOrders.audit.jsonl \
  --user-notes "It is safe to remove the filtering by IsArchived, which is obsolete." \
  --dry-run
```

## Configuration
Edit `config/default_config.toml`:
```toml
[api]
key = "sk-REDACTED"
endpoint = "https://api.example.com/refactor"

[defaults]
audit_log_dir = "./logs"
temp_output_dir = "./temp"

[database]
server = "localhost"
port = 1433
user = "my_user"
password = "my_password"
database = "my_database"
driver = "ODBC Driver 18 for SQL Server"
trust_server_certificate = true
```


## License
MIT