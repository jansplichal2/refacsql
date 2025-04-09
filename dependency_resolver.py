import pyodbc
import re
from typing import List, Dict, Any, Set


def get_connection(config: Dict[str, str]) -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},{config['port']};"
        f"DATABASE={config['database']};"
        f"UID={config['user']};"
        f"PWD={config['password']}"
    )
    return pyodbc.connect(conn_str)


def fetch_proc_definition(conn: pyodbc.Connection, proc_name: str, schema: str = 'dbo') -> str:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT m.definition
        FROM sys.sql_modules m
        JOIN sys.objects o ON m.object_id = o.object_id
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type = 'P' AND o.name = ? AND s.name = ?
    """, proc_name, schema)
    row = cursor.fetchone()
    return row.definition if row else ""


def fetch_table_columns(conn: pyodbc.Connection, table_name: str, schema: str = 'dbo') -> List[Dict[str, Any]]:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
    """, table_name, schema)
    return [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]


def fetch_function_definition(conn: pyodbc.Connection, func_name: str, schema: str = 'dbo') -> str:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT m.definition
        FROM sys.objects o
        JOIN sys.sql_modules m ON o.object_id = m.object_id
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type IN ('FN', 'IF', 'TF') AND o.name = ? AND s.name = ?
    """, func_name, schema)
    row = cursor.fetchone()
    return row.definition if row else ""


def fetch_table_type_columns(conn: pyodbc.Connection, type_name: str, schema: str = 'dbo') -> List[Dict[str, Any]]:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.name AS column_name, ty.name AS data_type, c.max_length, c.is_nullable
        FROM sys.table_types t
        JOIN sys.columns c ON t.type_table_object_id = c.object_id
        JOIN sys.types ty ON c.user_type_id = ty.user_type_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = ? AND s.name = ?
    """, type_name, schema)
    return [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]


def fetch_scalar_udt_info(conn: pyodbc.Connection, type_name: str) -> Dict[str, Any]:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.name, t.system_type_id, bt.name AS base_type, t.max_length
        FROM sys.types t
        LEFT JOIN sys.types bt ON t.system_type_id = bt.user_type_id
        WHERE t.is_user_defined = 1 AND t.name = ?
    """, type_name)
    row = cursor.fetchone()
    return dict(zip([column[0] for column in cursor.description], row)) if row else {}


def collect_dependencies(sql_text: str, conn: pyodbc.Connection, max_depth: int = 1, current_depth: int = 0,
                         visited: Set[str] = None) -> Dict[str, Any]:
    if visited is None:
        visited = set()

    context = {}

    # Simple regex-based extractors (placeholder logic)
    table_matches = re.findall(r"(?:FROM|JOIN|INTO|UPDATE|DELETE)\s+([\w]+)", sql_text, re.IGNORECASE)
    func_matches = re.findall(r"\b([\w]+)\s*\(", sql_text)
    proc_matches = re.findall(r"EXEC\s+(?:\[?dbo\]?\.)?([\w]+)", sql_text, re.IGNORECASE)

    for name in set(table_matches):
        key = f"table:{name.lower()}"
        if key not in visited:
            visited.add(key)
            columns = fetch_table_columns(conn, name)
            context[name] = {
                "type": "table",
                "columns": columns
            }

    for name in set(func_matches):
        key = f"func:{name.lower()}"
        if key not in visited:
            visited.add(key)
            definition = fetch_function_definition(conn, name)
            context[name] = {
                "type": "function",
                "definition": definition[:500]  # Truncated for context, optional
            }
            if current_depth < max_depth:
                nested = collect_dependencies(definition, conn, max_depth, current_depth + 1, visited)
                context.update(nested)

    for name in set(proc_matches):
        key = f"proc:{name.lower()}"
        if key not in visited:
            visited.add(key)
            definition = fetch_proc_definition(conn, name)
            context[name] = {
                "type": "procedure",
                "definition": definition[:500]  # Truncated for context
            }
            if current_depth < max_depth:
                nested = collect_dependencies(definition, conn, max_depth, current_depth + 1, visited)
                context.update(nested)

    return context
