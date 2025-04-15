import argparse
import json
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


def log_audit(entry: dict, path: str):
    with open(path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def build_prompt(proc_name: str, sql_text: str, context: dict) -> dict:
    return {
        "instruction": (
            "Please format, refactor, and optimize the stored procedure below using the provided metadata. "
            "Ensure the output is clean, logically structured, and avoids redundant or outdated constructs."
        ),
        "proc_name": proc_name,
        "sql": sql_text,
        "context": context
    }


def call_ai_refactor(api_key: str, endpoint: str, prompt: dict) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    response = requests.post(endpoint, headers=headers, json=prompt)
    response.raise_for_status()
    return response.json().get("refactored_sql", prompt["sql"]).strip()


def main():
    parser = argparse.ArgumentParser(description="AI-assisted T-SQL stored procedure refactoring tool.")
    parser.add_argument("--proc-name", required=True, help="Name of the stored procedure to refactor")
    parser.add_argument("--schema", default="dbo", help="Schema of the stored procedure")
    parser.add_argument("--audit-log", required=True, help="Path to write JSONL audit log")
    parser.add_argument("--depth", type=int, default=1, help="Recursion depth for dependency resolution")
    parser.add_argument("--config", default="config/default_config.toml", help="Path to the configuration file")
    parser.add_argument("--dry-run", action="store_true", help="Run without calling the API (just show parsed input)")

    args = parser.parse_args()
    config = load_config(args.config)

    Path(args.audit_log).parent.mkdir(parents=True, exist_ok=True)
    Path(config['defaults']['temp_output_dir']).mkdir(parents=True, exist_ok=True)

    conn = get_connection(config['database'])
    proc_sql = fetch_proc_definition(conn, args.proc_name, args.schema)

    if not proc_sql:
        print(f"Procedure {args.schema}.{args.proc_name} not found.")
        return

    print("Analyzing dependencies...")
    context = collect_dependencies_via_sys_views(conn, args.proc_name, args.schema, depth=args.depth)
    prompt = build_prompt(args.proc_name, proc_sql, context)

    if args.dry_run:
        print("\n=== DRY RUN ===")
        print("\n📄 Procedure SQL:")
        print(proc_sql.strip())
        print("\n📚 Dependency Context:")
        print(json.dumps(context, indent=2))
        print("\n🧠 Prompt:")
        print(json.dumps(prompt, indent=2))
        return

    try:
        print("Calling AI refactoring service...")
        refactored_sql = call_ai_refactor(
            api_key=config['api']['key'],
            endpoint=config['api']['endpoint'],
            prompt=prompt
        )
        log_audit({
            "proc_name": args.proc_name,
            "prompt": prompt,
            "response": {"refactored_sql": refactored_sql}
        }, args.audit_log)
        print("\n✅ Refactoring complete:\n")
        print(refactored_sql)
    except requests.HTTPError as e:
        print(f"❌ AI API request failed: {e}")


if __name__ == "__main__":
    main()
