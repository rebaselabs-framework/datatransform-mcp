"""
DataTransform MCP Server v1.1.0

Exposes the DataTransform API as MCP tools — agents can convert between data formats,
query data in natural language, filter/aggregate/sort/merge datasets, and profile data quality.

Usage:
    pip install datatransform-mcp
    # Configure in Claude Desktop:
    # {
    #   "mcpServers": {
    #     "datatransform": {
    #       "command": "uvx",
    #       "args": ["datatransform-mcp"],
    #       "env": {"DATATRANSFORM_API_KEY": "your-key-here"}
    #     }
    #   }
    # }
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional

import httpx
from mcp.server.fastmcp import FastMCP

# ── Config ────────────────────────────────────────────────────────────────────
API_BASE = os.environ.get("DATATRANSFORM_API_URL", "https://data-transform-api-v2.rebaselabs.online")
API_KEY = os.environ.get("DATATRANSFORM_API_KEY", "")
DEFAULT_TIMEOUT = 60.0

mcp = FastMCP(
    "datatransform",
    instructions=(
        "DataTransform handles data format conversion, filtering, aggregation, statistics, "
        "merging, pivoting, and natural-language data queries. "
        "Works with JSON, CSV, TSV, XML, YAML, and XLSX. "
        "Pass data as strings in the appropriate format."
    ),
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _headers() -> dict[str, str]:
    if not API_KEY:
        raise ValueError(
            "DATATRANSFORM_API_KEY environment variable is not set. "
            "Get a key at https://data-transform-api-v2.rebaselabs.online"
        )
    return {"X-API-Key": API_KEY, "Content-Type": "application/json"}


async def _post(path: str, body: dict) -> dict:
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        resp = await client.post(f"{API_BASE}{path}", json=body, headers=_headers())
        resp.raise_for_status()
        return resp.json()


def _out(result: dict) -> str:
    """Return the most useful field from a result."""
    for key in ("output", "result", "data", "answer", "profile"):
        if key in result and result[key] is not None:
            val = result[key]
            if isinstance(val, str):
                return val
            return json.dumps(val, indent=2)
    return json.dumps(result, indent=2)


# ── Tools ─────────────────────────────────────────────────────────────────────

@mcp.tool()
async def convert_format(
    data: str,
    input_format: str,
    output_format: str,
) -> str:
    """
    Convert data between formats: JSON, CSV, TSV, XML, YAML.
    Use to prepare data for different consumers or to normalize format.

    Examples:
    - CSV → JSON (to process with code)
    - JSON → CSV (to view in spreadsheet)
    - XML → JSON (to work with APIs)
    - JSON → YAML (for config files)

    Args:
        data: Input data as a string in the source format
        input_format: Source format — "json", "csv", "tsv", "xml", "yaml"
        output_format: Target format — "json", "csv", "tsv", "xml", "yaml"
    """
    result = await _post("/api/transform", {
        "data": data,
        "input_format": input_format,
        "output_format": output_format,
    })
    return _out(result)


@mcp.tool()
async def query_data(
    data: str,
    question: str,
    input_format: str = "json",
) -> str:
    """
    Ask a natural language question about your data and get an answer.
    Claude reads the data and answers in plain English with supporting values.

    Examples:
    - "What is the average revenue per customer?"
    - "Which products have the highest return rate?"
    - "How many rows have missing values in the email column?"
    - "What percentage of users are from the US?"

    Args:
        data: Data to query (JSON, CSV, or other format)
        question: Natural language question about the data
        input_format: Format of the data — "json", "csv", "tsv", "xml", "yaml"
    """
    result = await _post("/api/query", {
        "data": data,
        "question": question,
        "input_format": input_format,
    })
    return _out(result)


@mcp.tool()
async def filter_data(
    data: str,
    conditions: list[dict],
    input_format: str = "json",
    output_format: str = "json",
) -> str:
    """
    Filter rows by conditions. Returns only rows that match ALL conditions.

    Condition operators: "eq", "ne", "gt", "gte", "lt", "lte", "contains",
    "startswith", "endswith", "isnull", "notnull", "in", "not_in"

    Example conditions:
    [
        {"column": "age", "operator": "gte", "value": 18},
        {"column": "country", "operator": "eq", "value": "US"},
        {"column": "email", "operator": "notnull"}
    ]

    Args:
        data: Input data as string
        conditions: List of filter conditions (see above)
        input_format: Format of input data
        output_format: Format for filtered output
    """
    result = await _post("/api/filter", {
        "data": data,
        "conditions": conditions,
        "input_format": input_format,
        "output_format": output_format,
    })
    return _out(result)


@mcp.tool()
async def get_stats(
    data: str,
    columns: Optional[list[str]] = None,
    input_format: str = "json",
) -> str:
    """
    Compute statistics for each column: min, max, mean, median, std, null count, unique count.

    Args:
        data: Input dataset as string
        columns: Specific columns to analyze (default: all columns)
        input_format: Format of input data — "json", "csv", "tsv", etc.
    """
    body: dict = {"data": data, "input_format": input_format}
    if columns:
        body["columns"] = columns
    result = await _post("/api/stats", body)
    return _out(result)


@mcp.tool()
async def profile_data(
    data: str,
    input_format: str = "json",
) -> str:
    """
    Full data quality profile: type detection, completeness %, distribution,
    top values, outlier counts, and schema inference.
    Use to understand a new dataset quickly before processing it.

    Args:
        data: Input dataset as string
        input_format: Format of input data
    """
    result = await _post("/api/profile", {
        "data": data,
        "input_format": input_format,
    })
    return _out(result)


@mcp.tool()
async def aggregate_data(
    data: str,
    group_by: list[str],
    aggregations: list[dict],
    input_format: str = "json",
    output_format: str = "json",
) -> str:
    """
    GROUP BY aggregation — sum, min, max, mean, count per group.

    Example aggregations:
    [
        {"column": "revenue", "function": "sum", "alias": "total_revenue"},
        {"column": "user_id", "function": "count", "alias": "user_count"},
        {"column": "rating", "function": "mean", "alias": "avg_rating"}
    ]

    Args:
        data: Input dataset as string
        group_by: List of columns to group by
        aggregations: List of aggregation dicts with "column", "function", and optional "alias"
        input_format: Format of input data
        output_format: Format for output data
    """
    result = await _post("/api/aggregate", {
        "data": data,
        "group_by": group_by,
        "aggregations": aggregations,
        "input_format": input_format,
        "output_format": output_format,
    })
    return _out(result)


@mcp.tool()
async def sort_data(
    data: str,
    sort_by: list[dict],
    input_format: str = "json",
    output_format: str = "json",
) -> str:
    """
    Sort rows by one or more columns, ascending or descending.

    Example sort_by:
    [
        {"column": "created_at", "direction": "desc"},
        {"column": "name", "direction": "asc"}
    ]

    Args:
        data: Input dataset as string
        sort_by: List of dicts with "column" and "direction" ("asc" or "desc")
        input_format: Format of input data
        output_format: Format for sorted output
    """
    result = await _post("/api/sort", {
        "data": data,
        "sort_by": sort_by,
        "input_format": input_format,
        "output_format": output_format,
    })
    return _out(result)


@mcp.tool()
async def merge_datasets(
    left_data: str,
    right_data: str,
    on: list[str],
    how: str = "inner",
    input_format: str = "json",
    output_format: str = "json",
) -> str:
    """
    Join two datasets on key column(s). SQL-style joins.

    Join types:
    - "inner" — only rows with matching keys in both datasets
    - "left"  — all rows from left, matching from right (NaN if no match)
    - "right" — all rows from right, matching from left
    - "outer" — all rows from both datasets

    Args:
        left_data: Left dataset as string
        right_data: Right dataset as string
        on: List of column names to join on (must exist in both datasets)
        how: Join type — "inner", "left", "right", or "outer"
        input_format: Format of both input datasets
        output_format: Format for merged output
    """
    result = await _post("/api/merge", {
        "left_data": left_data,
        "right_data": right_data,
        "on": on,
        "how": how,
        "input_format": input_format,
        "output_format": output_format,
    })
    return _out(result)


@mcp.tool()
async def deduplicate_data(
    data: str,
    columns: Optional[list[str]] = None,
    input_format: str = "json",
    output_format: str = "json",
) -> str:
    """
    Remove duplicate rows from a dataset.

    Args:
        data: Input dataset as string
        columns: Columns to consider when detecting duplicates (default: all columns).
                 E.g. ["email"] removes rows with duplicate email values.
        input_format: Format of input data
        output_format: Format for deduplicated output
    """
    body: dict = {
        "data": data,
        "input_format": input_format,
        "output_format": output_format,
    }
    if columns:
        body["columns"] = columns
    result = await _post("/api/deduplicate", body)
    return _out(result)


@mcp.tool()
async def infer_schema(data: str, input_format: str = "json") -> str:
    """
    Infer the schema of a dataset — column names, types, required/optional, examples.
    Useful for understanding unknown data or generating documentation.

    Args:
        data: Input dataset as string
        input_format: Format of input data
    """
    result = await _post("/api/schema", {"data": data, "input_format": input_format})
    return _out(result)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    """Run the DataTransform MCP server via stdio transport."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
