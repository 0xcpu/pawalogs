"""
Field Analyzer

Analyzes database schemas to find identical or similar fields across tables
using Claude AI in headless mode.
"""

import argparse
import json
import subprocess
import sys
from hashlib import sha256
from pathlib import Path


def load_schemas(schemas_path: Path) -> dict:
    """Load the schemas.json file produced by db_schema_inspector."""
    if not schemas_path.exists():
        print(f"Error: Schemas file not found: {schemas_path}", file=sys.stderr)
        sys.exit(1)

    with open(schemas_path, "r") as f:
        return json.load(f)


def get_cache_path(schemas_path: Path) -> Path:
    """Generate cache file path based on schemas file content hash."""
    with open(schemas_path, "rb") as f:
        content_hash = sha256(f.read()).hexdigest()[:16]

    cache_dir = schemas_path.parent / ".pawalogs_cache"
    cache_dir.mkdir(exist_ok=True)
    return cache_dir / f"field_analysis_{content_hash}.json"


def load_cached_result(cache_path: Path) -> dict | None:
    """Load cached analysis result if it exists."""
    if cache_path.exists():
        with open(cache_path, "r") as f:
            return json.load(f)
    return None


def save_to_cache(cache_path: Path, result: dict):
    """Save analysis result to cache."""
    with open(cache_path, "w") as f:
        json.dump(result, f, indent=2)


def build_claude_prompt(schemas: dict) -> str:
    """Build the prompt for Claude to analyze field similarities."""
    prompt = """I have database schemas from multiple tables. Please analyze \
all fields across all tables and identify:

1. **Identical fields**: Fields that appear in multiple tables with the exact \
same name and type
2. **Similar fields**: Fields that likely represent the same data but have \
different names (e.g., "user_id" vs "userId" vs "uid")
3. **Type patterns**: Common field types and their usage patterns

Please output your analysis as a JSON object with this structure:

```json
{
  "identical_fields": [
    {
      "field_name": "id",
      "field_type": "INTEGER",
      "tables": ["table1", "table2", "table3"],
      "count": 3
    }
  ],
  "similar_fields": [
    {
      "group_description": "User identifiers",
      "fields": [
        {"table": "users", "field_name": "user_id", "field_type": "INTEGER"},
        {"table": "orders", "field_name": "userId", "field_type": "INTEGER"},
        {"table": "sessions", "field_name": "uid", "field_type": "INTEGER"}
      ],
      "similarity_reason": "All represent user foreign keys with similar naming"
    }
  ],
  "type_patterns": [
    {
      "field_type": "INTEGER",
      "common_names": ["id", "count", "timestamp"],
      "total_occurrences": 150
    }
  ],
  "summary": {
    "total_tables": 0,
    "total_fields": 0,
    "unique_field_names": 0,
    "unique_field_types": 0
  }
}
```

Here are the database schemas to analyze:

"""
    prompt += "```json\n"
    prompt += json.dumps(schemas, indent=2)
    prompt += "\n```\n\n"
    prompt += (
        "Please provide your analysis as valid JSON only, without any "
        "markdown formatting or explanations outside the JSON."
    )

    return prompt


def invoke_claude(prompt: str, verbose: bool = False) -> dict:
    """Invoke Claude in headless mode and return the parsed result."""
    cmd = [
        "claude",
        "-p",
        prompt,
        "--output-format",
        "json",
        "--append-system-prompt",
        (
            "You are a database expert analyzing field relationships. "
            "Always respond with valid JSON only."
        ),
    ]

    if verbose:
        print("Invoking Claude for field analysis...", file=sys.stderr)
        print(f"Command: {' '.join(cmd[:4])} <prompt>", file=sys.stderr)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        response = json.loads(result.stdout)

        if response.get("is_error"):
            print(
                f"Error from Claude: {response.get('result', 'Unknown error')}",
                file=sys.stderr,
            )
            sys.exit(1)

        if verbose:
            cost = response.get("total_cost_usd", 0)
            print(
                f"Claude response received (cost: ${cost:.4f})",
                file=sys.stderr,
            )

        # Extract the actual analysis from Claude's response
        result_text = response.get("result", "")

        # Try to parse the result as JSON
        # Claude might wrap it in markdown code blocks, so clean it up
        result_text = result_text.strip()
        if result_text.startswith("```json"):
            result_text = result_text[7:]
        if result_text.startswith("```"):
            result_text = result_text[3:]
        if result_text.endswith("```"):
            result_text = result_text[:-3]
        result_text = result_text.strip()

        analysis = json.loads(result_text)

        # Add metadata from Claude's response
        analysis["_metadata"] = {
            "cost_usd": response.get("total_cost_usd", 0),
            "duration_ms": response.get("duration_ms", 0),
            "num_turns": response.get("num_turns", 0),
            "session_id": response.get("session_id", ""),
        }

        return analysis

    except subprocess.CalledProcessError as e:
        print(f"Error invoking Claude: {e}", file=sys.stderr)
        print(f"stderr: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing Claude's response as JSON: {e}", file=sys.stderr)
        print(f"Response was: {result.stdout}", file=sys.stderr)
        sys.exit(1)


def main():
    """Entry point for the field analyzer."""
    parser = argparse.ArgumentParser(
        description=(
            "Analyze database schemas to find similar fields across tables "
            "using Claude AI"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This command analyzes the schemas.json file produced by pawalogs-db-inspector
and uses Claude AI in headless mode to identify identical and similar fields
across different tables.

Results are cached based on the schema file content. Use --force to regenerate.

Examples:
  # Analyze schemas and output to stdout
  pawalogs-field-analyzer output/schemas.json

  # Save analysis to a file
  pawalogs-field-analyzer output/schemas.json -o analysis.json

  # Force re-analysis even if cached result exists
  pawalogs-field-analyzer output/schemas.json --force

  # Verbose output showing Claude invocation details
  pawalogs-field-analyzer output/schemas.json --verbose
        """,
    )

    parser.add_argument(
        "schemas_path",
        help="Path to the schemas.json file produced by pawalogs-db-inspector",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output file path (default: print to stdout)",
        default=None,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force Claude invocation even if cached result exists",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    args = parser.parse_args()

    schemas_path = Path(args.schemas_path)

    if not schemas_path.exists():
        print(f"Error: Schemas file not found: {args.schemas_path}", file=sys.stderr)
        sys.exit(1)

    if args.verbose:
        print(f"Loading schemas from: {schemas_path}", file=sys.stderr)

    schemas = load_schemas(schemas_path)

    if args.verbose:
        print(f"Loaded {len(schemas)} table schemas", file=sys.stderr)

    cache_path = get_cache_path(schemas_path)
    analysis = None

    if not args.force:
        analysis = load_cached_result(cache_path)
        if analysis:
            if args.verbose:
                print(f"Using cached result from: {cache_path}", file=sys.stderr)

    if analysis is None:
        if args.verbose:
            print("No cached result found or --force specified", file=sys.stderr)

        prompt = build_claude_prompt(schemas)
        analysis = invoke_claude(prompt, verbose=args.verbose)

        save_to_cache(cache_path, analysis)
        if args.verbose:
            print(f"Cached result saved to: {cache_path}", file=sys.stderr)

    if args.output:
        output_path = Path(args.output)
        with open(output_path, "w") as f:
            json.dump(analysis, f, indent=2)
        print(f"Analysis written to: {output_path}")
    else:
        print(json.dumps(analysis, indent=2))


if __name__ == "__main__":
    main()
