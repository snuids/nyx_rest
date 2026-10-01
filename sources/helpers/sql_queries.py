"""Build parameterized queries for the generic SQL CRUD endpoint."""


def quote_identifier(name, db_type):
    if not isinstance(name, str) or not name or "\0" in name:
        raise ValueError("Invalid SQL identifier")
    if db_type == "sqlserver":
        return "[" + name.replace("]", "]]") + "]"
    if db_type == "postgres":
        return '"' + name.replace('"', '""') + '"'
    raise ValueError("Unsupported database type")


def build_crud_query(db_type, method, table, key_column, pkey, record=None):
    table = quote_identifier(table, db_type)
    key_column = quote_identifier(key_column, db_type)
    placeholder = "?" if db_type == "sqlserver" else "%s"

    if method == "get":
        return f"SELECT * FROM {table} WHERE {key_column} = {placeholder}", (pkey,)
    if method == "delete":
        return f"DELETE FROM {table} WHERE {key_column} = {placeholder}", (pkey,)
    if method != "post" or not isinstance(record, list) or not record:
        raise ValueError("Invalid CRUD request")

    columns = [quote_identifier(field["key"], db_type) for field in record]
    values = tuple(field["value"] for field in record)
    if pkey == "NEW":
        placeholders = ", ".join([placeholder] * len(columns))
        return (
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            values,
        )

    assignments = ", ".join(f"{column} = {placeholder}" for column in columns)
    return f"UPDATE {table} SET {assignments} WHERE {key_column} = {placeholder}", values + (pkey,)
