from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
DAILY_DIR = ROOT / "daily"
DAILY_DIR.mkdir(parents=True, exist_ok=True)

today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

content = f"""---
title: Daily Update {today}
---

# Daily Update — {today}

This is the first auto-generated daily summary page.

## Notes
- Replace this with fetched research/news summaries later.
- Keep the pipeline simple first.
"""

dated_file = DAILY_DIR / f"{today}.md"
latest_file = DAILY_DIR / "latest.md"

dated_file.write_text(content, encoding="utf-8")
latest_file.write_text(content, encoding="utf-8")

print(f"Generated: {dated_file}")
print(f"Updated: {latest_file}")
