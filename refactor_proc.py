import argparse
import json
import os
import subprocess
from pathlib import Path
from dependency_resolver import (
    get_connection,
    fetch_proc_definition,
    collect_dependencies_via_sys_views,
)
import tomli
import requests


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


def run_sqlfluff_fix(sql_text: str, config: dict) -> str:
    temp_file = Path(config['defaults']['temp_output_dir']) / "fix_input.sql"
    temp_file.write_text(sql_text)
    subprocess.run(
        [
            config['lint']['sqlfluff_path'],
            "fix",
            "--dialect",
            config['lint']['dialect'],
            str(temp_file)
        ],
        capture_output=True,
        text=True
    )
    return temp_file.read_text()


def log_audit(entry: dict, path: str):
    with open(path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def build_prompt(proc_name: str, sql_text: str, context: dict, lint_errors: list) -> dict:
    return {
        "instruction": "Please format, refactor, and optimize the stored procedure below using the provided metadata. Ensure the output is clean, logically structured, and avoids redundant or outdated constructs.",
        "proc_name": proc_name,
        "sql": sql_text,
        "context": context,
        "lint_errors": lint_errors
    }


def call_ai_refactor(api_key: str, endpoint: str, prompt: dict) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    response = requests.post(endpoint, headers=headers, json=prompt)
    response.raise_for_status()
    return response.json().get("refactored_sql", prompt["sql"])


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

    context = collect_dependencies_via_sys_views(conn, args.proc_name, args.schema, depth=args.depth)

    current_exchange = 0
    lint_failures = 0

    while current_exchange < args.max_exchanges:
        print(f"Exchange {current_exchange + 1}...")

        # Run sqlfluff on current SQL
        lint_issues = run_sqlfluff_check(proc_sql, config)

        # Build prompt
        prompt = build_prompt(args.proc_name, proc_sql, context, lint_issues)

        # Call AI refactoring API
        try:
            refactored_sql = call_ai_refactor(
                api_key=config['api']['key'],
                endpoint=config['api']['endpoint'],
                prompt=prompt
            )
        except requests.HTTPError as e:
            print(f"AI API request failed: {e}")
            break

        # Apply sqlfluff formatting
        formatted_sql = run_sqlfluff_fix(refactored_sql, config)

        # Run sqlfluff again to validate formatted SQL
        new_lint_issues = run_sqlfluff_check(formatted_sql, config)

        log_audit({
            "exchange": current_exchange + 1,
            "prompt": prompt,
            "response": {"refactored_sql": formatted_sql},
            "lint_issues": new_lint_issues,
            "context_keys": list(context.keys())
        }, args.audit_log)

        if not new_lint_issues:
            print("Lint passed. Final SQL produced.")
            break
        else:
            lint_failures += 1
            print(f"Lint found issues. Attempt {lint_failures}/3")
            if lint_failures >= config['lint']['max_lint_failures']:
                print("Too many consecutive lint failures. Aborting.")
                break

        proc_sql = formatted_sql
        current_exchange += 1


if __name__ == "__main__":
    main()
