from pathlib import Path
import json
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

RESEARCH_JSON_DIR = ROOT / "data" / "research" / "daily"
ENGLISH_JSON = ROOT / "data" / "english" / "daily" / "latest.json"
TECH_JSON = ROOT / "data" / "tech_news" / "daily" / "latest.json"
OUTPUT_HTML = ROOT / "index.html"


def load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def html_escape(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def get_latest_research_payload():
    if not RESEARCH_JSON_DIR.exists():
        return {}
    files = sorted(RESEARCH_JSON_DIR.glob("*.json"), reverse=True)
    if not files:
        return {}
    return load_json(files[0], {})


def render_research_section(research_payload):
    journals = research_payload.get("journals", {})
    cards = []

    for journal_name, items in journals.items():
        if not items:
            continue

        top_items = items[:2]
        item_html = ""
        for item in top_items:
            title = html_escape(item.get("title", "Untitled"))
            url = html_escape(item.get("url", "#"))
            method = html_escape(item.get("method", "N/A"))
            published = html_escape(item.get("published", "N/A"))
            citations = html_escape(item.get("citation_count", 0))

            item_html += f"""
            <div class="mini-item">
              <a href="{url}" target="_blank" rel="noopener noreferrer" class="mini-title">{title}</a>
              <div class="mini-meta">Published: {published} · Method: {method} · Citations: {citations}</div>
            </div>
            """

        cards.append(f"""
        <div class="panel-card">
          <div class="panel-card-header">
            <h3>{html_escape(journal_name)}</h3>
            <a href="./research/index.html">View more</a>
          </div>
          {item_html}
        </div>
        """)

    if not cards:
        cards_html = '<div class="panel-card"><p>No research items available yet.</p></div>'
    else:
        cards_html = "\n".join(cards[:4])

    return f"""
    <section class="section">
      <div class="section-header">
        <h2>Research Highlights</h2>
        <a href="./research/index.html">Open full digest</a>
      </div>
      <div class="card-grid two-col">
        {cards_html}
      </div>
    </section>
    """


def render_english_section(english_payload):
    topic = html_escape(english_payload.get("topic", "No topic yet"))
    intro = html_escape(english_payload.get("intro", "No practice content yet."))
    phrases = english_payload.get("speaking_lines", [])
    questions = english_payload.get("practice_questions", [])
    vocab = english_payload.get("keywords", [])

    phrases_html = "".join(f"<li>{html_escape(x)}</li>" for x in phrases) or "<li>No lines yet.</li>"
    questions_html = "".join(f"<li>{html_escape(x)}</li>" for x in questions) or "<li>No questions yet.</li>"
    vocab_html = "".join(f"<span class='tag'>{html_escape(x)}</span>" for x in vocab)

    return f"""
    <section class="section">
      <div class="section-header">
        <h2>Daily English Speaking Practice</h2>
      </div>
      <div class="panel-card large-card">
        <p class="muted-label">Topic of the day</p>
        <h3>{topic}</h3>
        <p>{intro}</p>

        <div class="split-grid">
          <div>
            <h4>Speaking lines</h4>
            <ul>
              {phrases_html}
            </ul>
          </div>
          <div>
            <h4>Practice questions</h4>
            <ul>
              {questions_html}
            </ul>
          </div>
        </div>

        <div class="tag-wrap">
          {vocab_html}
        </div>
      </div>
    </section>
    """


def render_tech_section(tech_payload):
    items = tech_payload.get("items", [])
    if not items:
        cards_html = '<div class="panel-card"><p>No tech news available yet.</p></div>'
    else:
        cards = []
        for item in items[:5]:
            title = html_escape(item.get("title", "Untitled"))
            url = html_escape(item.get("url", "#"))
            source = html_escape(item.get("source", "Unknown"))
            summary = html_escape(item.get("summary", ""))
            published = html_escape(item.get("published", ""))

            cards.append(f"""
            <div class="panel-card">
              <div class="panel-card-header">
                <h3><a href="{url}" target="_blank" rel="noopener noreferrer">{title}</a></h3>
              </div>
              <div class="mini-meta">{source} · {published}</div>
              <p>{summary}</p>
            </div>
            """)
        cards_html = "\n".join(cards)

    return f"""
    <section class="section">
      <div class="section-header">
        <h2>Latest Tech News</h2>
      </div>
      <div class="card-grid">
        {cards_html}
      </div>
    </section>
    """


def build_page():
    research_payload = get_latest_research_payload()
    english_payload = load_json(ENGLISH_JSON, {})
    tech_payload = load_json(TECH_JSON, {})

    today = datetime.now().strftime("%Y-%m-%d")

    html_text = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Daily Intelligence Hub</title>
  <style>
    :root {{
      --bg: #f6f8fb;
      --card: #ffffff;
      --text: #1c2430;
      --muted: #657084;
      --line: #dde4ee;
      --accent: #2f6fed;
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      font-family: Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.6;
    }}

    .container {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 28px 20px 60px;
    }}

    .hero {{
      background: linear-gradient(135deg, #ffffff 0%, #eef4ff 100%);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 30px;
      margin-bottom: 28px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.04);
    }}

    .hero h1 {{
      margin: 0 0 10px;
      font-size: 2rem;
    }}

    .hero p {{
      margin: 0;
      color: var(--muted);
      max-width: 720px;
    }}

    .hero-meta {{
      margin-top: 14px;
      font-size: 0.95rem;
      color: var(--muted);
    }}

    .nav-links {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 18px;
    }}

    .nav-links a {{
      text-decoration: none;
      color: var(--text);
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 8px 14px;
    }}

    .section {{
      margin-bottom: 34px;
    }}

    .section-header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 14px;
      gap: 12px;
    }}

    .section-header h2 {{
      margin: 0;
      font-size: 1.35rem;
    }}

    .section-header a {{
      color: var(--accent);
      text-decoration: none;
      font-size: 0.95rem;
    }}

    .card-grid {{
      display: grid;
      gap: 16px;
      grid-template-columns: 1fr;
    }}

    .two-col {{
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    }}

    .panel-card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 6px 18px rgba(0,0,0,0.03);
    }}

    .large-card {{
      padding: 22px;
    }}

    .panel-card-header {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
      margin-bottom: 10px;
    }}

    .panel-card-header h3 {{
      margin: 0;
      font-size: 1.05rem;
    }}

    .panel-card-header a,
    .mini-title {{
      color: var(--accent);
      text-decoration: none;
    }}

    .mini-item {{
      padding: 10px 0;
      border-top: 1px solid var(--line);
    }}

    .mini-item:first-of-type {{
      border-top: none;
      padding-top: 0;
    }}

    .mini-meta {{
      color: var(--muted);
      font-size: 0.9rem;
      margin-top: 4px;
    }}

    .muted-label {{
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      font-size: 0.78rem;
      margin-bottom: 6px;
    }}

    .split-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 18px;
      margin-top: 14px;
    }}

    .tag-wrap {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 16px;
    }}

    .tag {{
      display: inline-block;
      padding: 6px 10px;
      background: #eef3ff;
      border: 1px solid #d8e3ff;
      border-radius: 999px;
      font-size: 0.85rem;
      color: #3553a6;
    }}

    footer {{
      margin-top: 40px;
      color: var(--muted);
      font-size: 0.92rem;
      text-align: center;
    }}
  </style>
</head>
<body>
  <div class="container">
    <section class="hero">
      <h1>Daily Intelligence Hub</h1>
      <p>Research, language practice, and technology updates in one place.</p>
      <div class="hero-meta">Updated: {today}</div>
      <div class="nav-links">
        <a href="./research/index.html">Research Digest</a>
        <a href="./research/archive/index.html">Research Archive</a>
      </div>
    </section>

    {render_research_section(research_payload)}
    {render_english_section(english_payload)}
    {render_tech_section(tech_payload)}

    <footer>
      Daily dashboard generated automatically.
    </footer>
  </div>
</body>
</html>
"""
    OUTPUT_HTML.write_text(html_text, encoding="utf-8")
    print(f"Built homepage: {OUTPUT_HTML}")


if __name__ == "__main__":
    build_page()
