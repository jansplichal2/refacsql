import argparse
import json
import os
import subprocess
from pathlib import Path
from dependency_resolver import (
    get_connection,
    fetch_proc_definition,
)
import tomli


def load_config(path: str = "config/default_config.toml") -> dict:
    with open(path, "rb") as f:
        return tomli.load(f)


def run_sqlfluff_check(sql_text: str, config: dict) -> list:
    temp_file = Path(config['defaults']['temp_output_dir']) / "lint_check.sql"
    temp_file.write_text(sql_text)
    result = subprocess.run(
        [
            config['lint']['sqlfluff_path'],
            "lint",
            "--dialect",
            config['lint']['dialect'],
            "--format",
            "json",
            str(temp_file),
        ],
        capture_output=True,
        text=True,
    )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return []


def log_audit(entry: dict, path: str):
    with open(path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def main():
    parser = argparse.ArgumentParser(description="AI-assisted T-SQL stored procedure refactoring tool.")
    parser.add_argument("--proc-name", required=True, help="Name of the stored procedure to refactor")
    parser.add_argument("--schema", default="dbo", help="Schema of the stored procedure")
    parser.add_argument("--max-exchanges", type=int, default=3, help="Maximum number of back-and-forth exchanges")
    parser.add_argument("--audit-log", required=True, help="Path to write JSONL audit log")
    parser.add_argument("--depth", type=int, default=1, help="Recursion depth for dependency resolution")
    parser.add_argument("--config", default="config/default_config.toml", help="Path to the configuration file")

    args = parser.parse_args()
    config = load_config(args.config)

    # Ensure audit/log/temp dirs exist
    Path(args.audit_log).parent.mkdir(parents=True, exist_ok=True)
    Path(config['defaults']['temp_output_dir']).mkdir(parents=True, exist_ok=True)

    conn = get_connection(config['database'])
    proc_sql = fetch_proc_definition(conn, args.proc_name, args.schema)

    if not proc_sql:
        print(f"Procedure {args.schema}.{args.proc_name} not found.")
        return

    current_exchange = 0
    lint_failures = 0

    while current_exchange < args.max_exchanges:
        print(f"Exchange {current_exchange + 1}...")

        # Placeholder: AI logic goes here
        refactored_sql = proc_sql  # Replace with AI call later

        # Run sqlfluff
        lint_issues = run_sqlfluff_check(refactored_sql, config)
        log_audit({
            "exchange": current_exchange + 1,
            "input_sql": proc_sql,
            "output_sql": refactored_sql,
            "lint_issues": lint_issues
        }, args.audit_log)

        if not lint_issues:
            print("Lint passed. Final SQL produced.")
            break
        else:
            lint_failures += 1
            print(f"Lint found issues. Attempt {lint_failures}/3")
            if lint_failures >= config['lint']['max_lint_failures']:
                print("Too many consecutive lint failures. Aborting.")
                break

        current_exchange += 1


if __name__ == "__main__":
    main()
