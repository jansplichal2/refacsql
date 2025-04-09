import pyodbc
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


def collect_dependencies_via_sys_views(conn: pyodbc.Connection, proc_name: str, schema: str = 'dbo', depth: int = 1,
                                       visited: Set[str] = None) -> Dict[str, Any]:
    if visited is None:
        visited = set()

    context = {}
    key = f"{schema}.{proc_name}".lower()
    if key in visited or depth < 0:
        return context
    visited.add(key)

    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            dep.referenced_entity_name,
            dep.referenced_schema_name,
            obj.type_desc
        FROM sys.sql_expression_dependencies dep
        JOIN sys.objects obj ON dep.referenced_id = obj.object_id
        JOIN sys.objects caller ON dep.referencing_id = caller.object_id
        JOIN sys.schemas s ON caller.schema_id = s.schema_id
        WHERE caller.name = ? AND s.name = ? AND obj.is_ms_shipped = 0
    """, proc_name, schema)

    for row in cursor.fetchall():
        name, ref_schema, obj_type = row
        obj_type = obj_type.upper()
        ref_key = f"{ref_schema}.{name}".lower()
        if ref_key in visited:
            continue

        try:
            if obj_type in ("USER_TABLE", "VIEW"):
                columns = fetch_table_columns(conn, name, ref_schema)
                context[name] = {
                    "type": "table" if obj_type == "USER_TABLE" else "view",
                    "columns": columns
                }
            elif "FUNCTION" in obj_type:
                definition = fetch_function_definition(conn, name, ref_schema)
                context[name] = {
                    "type": "function",
                    "definition": definition[:500]
                }
                nested = collect_dependencies_via_sys_views(conn, name, ref_schema, depth - 1, visited)
                context.update(nested)
            elif obj_type == "SQL_STORED_PROCEDURE":
                definition = fetch_proc_definition(conn, name, ref_schema)
                context[name] = {
                    "type": "procedure",
                    "definition": definition[:500]
                }
                nested = collect_dependencies_via_sys_views(conn, name, ref_schema, depth - 1, visited)
                context.update(nested)
        except Exception as e:
            context[name] = {"error": str(e)}

    return context
