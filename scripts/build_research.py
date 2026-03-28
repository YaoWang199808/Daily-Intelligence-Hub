from pathlib import Path
from utils import ROOT, ensure_dir, load_json, today_str

DATA_DIR = ROOT / "data" / "research" / "daily"
RESEARCH_DIR = ROOT / "research"
ARCHIVE_DIR = RESEARCH_DIR / "archive"


def html_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def render_item(item):
    authors = ", ".join(item.get("authors", [])) or "N/A"
    institutions = ", ".join(item.get("institution", [])) or "N/A"
    keywords = ", ".join(item.get("keywords", [])) or "N/A"

    summary_html = "".join(f"<li>{html_escape(s)}</li>" for s in item.get("summary", []))
    conclusions_html = "".join(f"<li>{html_escape(s)}</li>" for s in item.get("conclusions", []))

    return f"""
    <article class="card">
      <h3><a href="{item['url']}" target="_blank" rel="noopener noreferrer">{html_escape(item['title'])}</a></h3>
      <p><strong>Authors:</strong> {html_escape(authors)}</p>
      <p><strong>Institution:</strong> {html_escape(institutions)}</p>
      <p><strong>Published:</strong> {html_escape(item['published'])}</p>
      <p><strong>Keywords:</strong> {html_escape(keywords)}</p>
      <p><strong>Method:</strong> {html_escape(item['method'])}</p>
      <p><strong>Source:</strong> {html_escape(item['source'])}</p>
      <div>
        <strong>Summary:</strong>
        <ul>{summary_html}</ul>
      </div>
      <div>
        <strong>Key conclusions:</strong>
        <ul>{conclusions_html}</ul>
      </div>
    </article>
    """


def render_topic_section(topic_name, items):
    if not items:
        body = '<p class="empty">No new items found for today.</p>'
    else:
        body = "\n".join(render_item(item) for item in items)

    anchor = topic_name.lower().replace(" ", "-")
    return f"""
    <section id="{anchor}" class="topic-section">
      <h2>{html_escape(topic_name)}</h2>
      {body}
    </section>
    """


def render_page(title, page_heading, date_str, topics, archive_links):
    nav_links = "".join(
        f'<a class="tab" href="#{topic.lower().replace(" ", "-")}">{html_escape(topic)}</a>'
        for topic in topics.keys()
    )

    topic_sections = "\n".join(
        render_topic_section(topic, items) for topic, items in topics.items()
    )

    archive_html = "".join(
        f'<li><a href="./archive/{d}.html">{d}</a></li>' for d in archive_links
    ) or "<li>No archive yet.</li>"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{html_escape(title)}</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      max-width: 1100px;
      margin: 0 auto;
      padding: 24px;
      line-height: 1.6;
    }}
    h1 {{
      margin-bottom: 8px;
    }}
    .topbar {{
      margin-bottom: 24px;
    }}
    .tabs {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 16px 0 28px;
    }}
    .tab {{
      padding: 8px 12px;
      border: 1px solid #ccc;
      border-radius: 999px;
      text-decoration: none;
      color: #222;
      background: #f7f7f7;
    }}
    .topic-section {{
      margin-bottom: 40px;
    }}
    .card {{
      border: 1px solid #ddd;
      border-radius: 10px;
      padding: 16px;
      margin: 16px 0;
      background: #fff;
    }}
    .card h3 {{
      margin-top: 0;
    }}
    .archive {{
      margin-top: 40px;
      padding-top: 16px;
      border-top: 2px solid #eee;
    }}
    .empty {{
      color: #666;
      font-style: italic;
    }}
    a {{
      color: #0a58ca;
    }}
  </style>
</head>
<body>
  <div class="topbar">
    <h1>{html_escape(page_heading)}</h1>
    <p><strong>Date:</strong> {date_str}</p>
    <p><a href="../index.html">Home</a></p>
  </div>

  <div class="tabs">
    {nav_links}
  </div>

  {topic_sections}

  <div class="archive">
    <h2>Previous Updates</h2>
    <ul>
      {archive_html}
    </ul>
  </div>
</body>
</html>
"""


def render_archive_index(archive_dates):
    items = "".join(
        f'<li><a href="./{d}.html">{d}</a></li>' for d in archive_dates
    ) or "<li>No archive yet.</li>"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Research Archive</title>
</head>
<body>
  <h1>Research Archive</h1>
  <p><a href="../index.html">Back to Research</a></p>
  <ul>{items}</ul>
</body>
</html>
"""


def main():
    ensure_dir(RESEARCH_DIR)
    ensure_dir(ARCHIVE_DIR)

    today = today_str()
    today_json = DATA_DIR / f"{today}.json"

    if not today_json.exists():
      raise FileNotFoundError(f"Missing daily research data: {today_json}")

    payload = load_json(today_json, {})
    topics = payload.get("topics", {})

    archive_dates = sorted(
        [p.stem for p in DATA_DIR.glob("*.json") if p.stem != today],
        reverse=True
    )

    research_index = render_page(
        title=f"Research Digest {today}",
        page_heading="Research Digest",
        date_str=today,
        topics=topics,
        archive_links=archive_dates[:30]
    )

    archive_page = render_page(
        title=f"Research Archive {today}",
        page_heading="Research Digest Archive",
        date_str=today,
        topics=topics,
        archive_links=archive_dates[:30]
    )

    (RESEARCH_DIR / "index.html").write_text(research_index, encoding="utf-8")
    (ARCHIVE_DIR / f"{today}.html").write_text(archive_page, encoding="utf-8")
    (ARCHIVE_DIR / "index.html").write_text(
        render_archive_index([today] + archive_dates[:59]),
        encoding="utf-8"
    )

    print(f"Built research/index.html")
    print(f"Built research/archive/{today}.html")
    print(f"Built research/archive/index.html")


if __name__ == "__main__":
    main()
