import os

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


def schema_summary(schema: pa.Schema) -> str:
    cols = ", ".join(f"{f.name}: {f.type}" for f in schema)
    return f"{len(schema)} columns ({cols})"


def print_sample_rows(table: pa.Table, limit: int = 10) -> None:
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
