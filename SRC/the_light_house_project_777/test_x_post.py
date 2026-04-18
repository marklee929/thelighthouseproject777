from __future__ import annotations

import json
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if load_dotenv is not None:
    load_dotenv(PROJECT_ROOT / ".env", override=False)

from integrations.x_client import XClient  # noqa: E402


def main() -> int:
    text = "OAuth1 posting test from the_light_house_project+777"
    if len(sys.argv) > 1:
        text = " ".join(str(arg) for arg in sys.argv[1:]).strip() or text

    client = XClient()
    validation = client.validate_x_oauth1_config()

    print("[X-POST-TEST] validation", json.dumps(validation, ensure_ascii=False), flush=True)
    result = client.post_text_to_x(text)
    print("[X-POST-TEST] result", json.dumps(result, ensure_ascii=False), flush=True)

    return 0 if bool(result.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
