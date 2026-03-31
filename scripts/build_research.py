from pathlib import Path
from utils import ROOT, ensure_dir, load_json, today_str

DATA_DIR = ROOT / "data" / "research" / "daily"
RESEARCH_DIR = ROOT / "research"
ARCHIVE_DIR = RESEARCH_DIR / "archive"


def html_escape(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def render_item(item):
    authors = ", ".join(item.get("authors", [])) or "N/A"
    institutions_list = item.get("institutions", []) or item.get("institution", []) or []
    institutions = ", ".join(institutions_list)
    journal = item.get("journal", "") or "N/A"
    abstract_text = item.get("abstract", "") or "Abstract not available."
    citations = item.get("citation_count", 0)

    institution_html = ""
    if institutions:
        institution_html = f"<p><strong>Institution:</strong> {html_escape(institutions)}</p>"

    return f"""
    <article class="card">
      <h4><a href="{item['url']}" target="_blank" rel="noopener noreferrer">{html_escape(item['title'])}</a></h4>
      <p><strong>Authors:</strong> {html_escape(authors)}</p>
      {institution_html}
      <p><strong>Published:</strong> {html_escape(item.get('published', 'N/A'))}</p>
      <p><strong>Journal:</strong> {html_escape(journal)}</p>
      <p><strong>Method:</strong> {html_escape(item.get('method', 'N/A'))}</p>
      <p><strong>Citations:</strong> {html_escape(citations)}</p>
      <div>
        <strong>Abstract:</strong>
        <p>{html_escape(abstract_text)}</p>
      </div>
    </article>
    """


def render_journal_section(journal_name, items):
    anchor = (
        journal_name.lower()
        .replace(" ", "-")
        .replace("&", "and")
        .replace(",", "")
    )

    if not items:
        body = '<p class="empty">No items in this section.</p>'
    else:
        body = "\n".join(render_item(item) for item in items)

    return f"""
    <section id="{anchor}" class="journal-section">
      <h2>{html_escape(journal_name)}</h2>
      {body}
    </section>
    """


def render_page(title, page_heading, date_str, journals, archive_links):
    nav_links = "".join(
        f'<a class="tab" href="#{journal.lower().replace(" ", "-").replace("&", "and").replace(",", "")}">{html_escape(journal)}</a>'
        for journal in journals.keys()
    )

    journal_sections = "\n".join(
        render_journal_section(journal, items) for journal, items in journals.items()
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
      max-width: 1120px;
      margin: 0 auto;
      padding: 24px;
      line-height: 1.6;
      color: #222;
    }}
    .topbar {{
      margin-bottom: 20px;
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
    .journal-section {{
      margin-bottom: 46px;
    }}
    .card {{
      border: 1px solid #ddd;
      border-radius: 10px;
      padding: 16px;
      margin: 14px 0;
      background: #fff;
    }}
    .card h4 {{
      margin-top: 0;
      margin-bottom: 8px;
      font-size: 1.05rem;
    }}
    .archive {{
      margin-top: 50px;
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
    <p><strong>Date:</strong> {html_escape(date_str)}</p>
    <p><a href="../index.html">Home</a></p>
  </div>

  <div class="tabs">
    {nav_links}
  </div>

  {journal_sections}

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
    journals = payload.get("journals", {})

    archive_dates = sorted(
        [p.stem for p in DATA_DIR.glob("*.json") if p.stem != today],
        reverse=True
    )

    research_index = render_page(
        title=f"Research Digest {today}",
        page_heading="Research Digest",
        date_str=today,
        journals=journals,
        archive_links=archive_dates[:30]
    )

    archive_page = render_page(
        title=f"Research Archive {today}",
        page_heading="Research Digest Archive",
        date_str=today,
        journals=journals,
        archive_links=archive_dates[:30]
    )

    (RESEARCH_DIR / "index.html").write_text(research_index, encoding="utf-8")
    (ARCHIVE_DIR / f"{today}.html").write_text(archive_page, encoding="utf-8")
    (ARCHIVE_DIR / "index.html").write_text(
        render_archive_index([today] + archive_dates[:59]),
        encoding="utf-8"
    )

    print("Built research pages.")


if __name__ == "__main__":
    main()
