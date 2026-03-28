from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
DAILY_DIR = ROOT / "daily"
DAILY_DIR.mkdir(parents=True, exist_ok=True)

today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Daily Update {today}</title>
</head>
<body>
  <h1>Daily Update — {today}</h1>
  <p>This is the first auto-generated daily summary page.</p>
  <ul>
    <li>Replace this with fetched research/news summaries later.</li>
    <li>Keep the pipeline simple first.</li>
  </ul>
</body>
</html>
"""

dated_file = DAILY_DIR / f"{today}.html"
latest_file = DAILY_DIR / "latest.html"

dated_file.write_text(html, encoding="utf-8")
latest_file.write_text(html, encoding="utf-8")

print(f"Generated: {dated_file}")
print(f"Updated: {latest_file}")
