# DataTransform MCP Server

**Data transformation and analysis for AI agents — via MCP.**

[DataTransform API](https://data-transfer-api.rebaselabs.online) converts between data formats, filters/aggregates/merges datasets, and answers natural language questions about your data. This MCP server makes those capabilities available to any Claude, ChatGPT, or MCP-compatible agent.

## Tools

| Tool | Description |
|------|-------------|
| `convert_format` | Convert between JSON, CSV, TSV, XML, YAML |
| `query_data` | Ask natural language questions about data |
| `filter_data` | Filter rows by conditions (eq, gt, contains, etc.) |
| `get_stats` | Column statistics (min, max, mean, std, nulls) |
| `profile_data` | Full data quality profile |
| `aggregate_data` | GROUP BY with sum/mean/count/min/max |
| `sort_data` | Sort by one or more columns asc/desc |
| `merge_datasets` | SQL-style joins (inner/left/right/outer) |
| `deduplicate_data` | Remove duplicate rows |
| `infer_schema` | Detect column types and schema |

## Installation

```bash
pip install datatransform-mcp
```

## Claude Desktop Configuration

```json
{
  "mcpServers": {
    "datatransform": {
      "command": "uvx",
      "args": ["datatransform-mcp"],
      "env": {
        "DATATRANSFORM_API_KEY": "your-api-key-here"
      }
    }
  }
}
```

## Usage Examples

```
"Convert this CSV to JSON"
→ convert_format(csv_data, "csv", "json")

"What is the average order value by customer segment?"
→ query_data(data, "average order value by customer segment")

"Give me only rows where status = 'active' and age >= 18"
→ filter_data(data, [{"column":"status","operator":"eq","value":"active"}, ...])

"Profile this dataset for data quality issues"
→ profile_data(data)

"Join customers and orders on customer_id"
→ merge_datasets(customers, orders, ["customer_id"], how="left")
```

## License

MIT
