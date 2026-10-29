from __future__ import annotations

import json
import sys
import uuid


def log_http_error(method: str, target: str, status: int, elapsed_us: int) -> None:
    """Emit the same compact, structured fields as the C++ HTTP error logger."""
    event = {
        "event": "http_request_error",
        "request_id": uuid.uuid4().hex,
        "method": method,
        "target": target,
        "status": status,
        "elapsed_us": elapsed_us,
    }
    print(json.dumps(event, ensure_ascii=False, separators=(",", ":")), file=sys.stdout, flush=True)
