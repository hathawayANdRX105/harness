#!/usr/bin/env python3
"""Minimal stdio MCP server fixture for config-wiring integration tests.

Answers `initialize` and `tools/list`, ignores notifications. Kept as a file
so the Rust test body stays focused on config -> spec -> runtime assertions.
"""
import json
import sys

REPLIES = {
    "initialize": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "serverInfo": {"name": "trivial", "version": "1.0.0"},
    },
    "tools/list": {
        "tools": [
            {
                "name": "echo",
                "description": "A trivial echo tool.",
                "inputSchema": {"type": "object", "properties": {}},
            }
        ],
    },
}


def main():
    out = sys.stdout
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue
        rid = req.get("id")
        if rid is None:
            continue  # notification: no reply expected
        result = REPLIES.get(req.get("method"), {})
        out.write(json.dumps({"jsonrpc": "2.0", "id": rid, "result": result}) + "\n")
        out.flush()


main()
