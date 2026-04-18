from __future__ import annotations

import argparse
import json
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

from social_automation.facebook_publisher import FacebookPublisher  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual Facebook Page token reissue flow.")
    parser.add_argument("--app-id", required=True, help="Facebook App ID")
    parser.add_argument("--app-secret", required=True, help="Facebook App Secret")
    parser.add_argument("--page-id", required=True, help="Target Facebook Page ID")
    parser.add_argument("--user-short-lived-token", required=True, help="Short-lived Facebook user token")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    publisher = FacebookPublisher(str(PROJECT_ROOT))
    result = publisher.reissue_facebook_page_token(
        app_id=args.app_id,
        app_secret=args.app_secret,
        user_short_lived_token=args.user_short_lived_token,
        target_page_id=args.page_id,
    )
    print("[FACEBOOK-REISSUE] result", json.dumps(result, ensure_ascii=False), flush=True)
    return 0 if bool(result.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
