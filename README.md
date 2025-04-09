`refacsql` is a command-line tool for AI-assisted refactoring of legacy T-SQL stored procedures. It performs dependency analysis, verifies correctness with `sqlfluff`, and maintains a detailed audit of all exchanges.

## Features
- Refactor large legacy stored procedures using AI
- Auto-detect and request missing table/function/type metadata
- Validate syntax using `sqlfluff`
- Recursively resolve dependencies (configurable depth)
- Log all interactions and decisions in a JSONL audit file

## Install
```bash
pip install -r requirements.txt
```

## Usage
```bash
python refactor_proc.py \
  --proc-name usp_ProcessOrders \
  --max-exchanges 3 \
  --audit-log ./logs/usp_ProcessOrders.audit.jsonl
```

## Configuration
Edit `config/default_config.toml`:
```toml
[api]
key = "sk-REDACTED"
endpoint = "https://api.example.com/refactor"

[defaults]
max_exchanges = 3
sql_dialect = "tsql"
audit_log_dir = "./logs"
temp_output_dir = "./temp"

[lint]
enabled = true
max_lint_failures = 3
dialect = "tsql"
sqlfluff_path = "sqlfluff"

[database]
server = "localhost"
port = 1433
user = "my_user"
password = "my_password"
database = "my_database"
driver = "ODBC Driver 17 for SQL Server"
```

```
sqlfluff fix --dialect tsql path/to/file.sql
```

## License
MIT