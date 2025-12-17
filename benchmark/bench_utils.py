import os

from typing import TYPE_CHECKING, Any, Iterable, Sequence

if TYPE_CHECKING:
    import pyarrow as pa


def print_section(title: str) -> None:
    line = "=" * len(title)
    print(f"\n{title}\n{line}")


def print_kv(key: str, value: object) -> None:
    print(f"- {key}: {value}")


def format_elapsed(seconds: float) -> str:
    seconds = max(0.0, float(seconds))
    minutes = int(seconds // 60)
    rem = seconds - (minutes * 60)
    if minutes > 0:
        return f"{minutes}m {rem:.3f}s"
    return f"{rem:.3f}s"


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def schema_summary(schema: "pa.Schema") -> str:
    cols = ", ".join(f"{f.name}: {f.type}" for f in schema)
    return f"{len(schema)} columns ({cols})"


def print_sample_rows(table: "pa.Table", limit: int = 10) -> None:
    import pyarrow as pa

    if table.num_rows == 0:
        print("(no rows)")
        return

    head = table.slice(0, min(limit, table.num_rows))
    rows = head.to_pylist()
    if not rows:
        print("(no rows)")
        return

    columns = list(head.schema.names)
    cell_strings = {c: [str(r.get(c, "")) for r in rows] for c in columns}
    widths = {c: max(len(c), *(len(v) for v in cell_strings[c])) for c in columns}

    def fmt_row(values: list[str]) -> str:
        return " | ".join(v.ljust(widths[c]) for c, v in zip(columns, values))

    print(fmt_row(columns))
    print("-+-".join("-" * widths[c] for c in columns))
    for i in range(len(rows)):
        print(fmt_row([cell_strings[c][i] for c in columns]))


def _normalize_dbapi_type(type_code: object) -> str:
    if type_code is None:
        return ""
    if isinstance(type_code, str):
        return type_code
    name = getattr(type_code, "name", None)
    if isinstance(name, str) and name:
        return name
    return str(type_code)


def dbapi_schema_summary(description: Sequence[Sequence[Any]] | None) -> str:
    if not description:
        return "0 columns"
    cols = ", ".join(f"{col[0]}: {_normalize_dbapi_type(col[1] if len(col) > 1 else None)}" for col in description)
    return f"{len(description)} columns ({cols})"


def print_sample_rows_dbapi(description: Sequence[Sequence[Any]] | None, rows: Iterable[Sequence[Any]], limit: int = 10) -> None:
    if not description:
        print("(no columns)")
        return

    column_names = [str(col[0]) for col in description]
    materialized: list[Sequence[Any]] = []
    for row in rows:
        materialized.append(row)
        if len(materialized) >= limit:
            break

    if not materialized:
        print("(no rows)")
        return

    def cell(value: Any) -> str:
        if value is None:
            return "NULL"
        return str(value)

    cell_strings = {
        name: [cell(row[i]) if i < len(row) else "" for row in materialized]
        for i, name in enumerate(column_names)
    }
    widths = {name: max(len(name), *(len(v) for v in values)) for name, values in cell_strings.items()}

    def fmt_row(values: list[str]) -> str:
        return " | ".join(v.ljust(widths[name]) for name, v in zip(column_names, values))

    print(fmt_row(column_names))
    print("-+-".join("-" * widths[name] for name in column_names))
    for i in range(len(materialized)):
        print(fmt_row([cell_strings[name][i] for name in column_names]))
