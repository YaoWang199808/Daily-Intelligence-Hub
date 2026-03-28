from __future__ import annotations

import argparse
import html
import json
import sys
from collections import OrderedDict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Tuple


def try_import_project_utils() -> Tuple[Path | None, Any, Any, Any]:
    try:
        from utils import ROOT, ensure_dir, load_json, today_str  # type: ignore
        return ROOT, ensure_dir, load_json, today_str
    except Exception:
        return None, None, None, None


ROOT, ensure_dir, load_json, today_str = try_import_project_utils()

if ROOT is not None:
    DATA_DIR = ROOT / "data" / "research" / "daily"
    RESEARCH_DIR = ROOT / "research"
    ARCHIVE_DIR = RESEARCH_DIR / "archive"
else:
    DATA_DIR = Path("output/daily")
    RESEARCH_DIR = Path("output/research")
    ARCHIVE_DIR = RESEARCH_DIR / "archive"


DEFAULT_JOURNAL_ORDER = [
    "Mechanical Systems and Signal Processing",
    "Tunnelling and Underground Space Technology",
    "Engineering Structures",
    "Measurement",
    "NDT & E International",
    "Expert Systems with Applications",
    "Rock Mechanics and Rock Engineering",
]


def html_escape(text: Any) -> str:
    return html.escape(str(text or ""), quote=True)



def normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    def split_people(value: Any) -> List[str]:
        if isinstance(value, list):
            return [str(x).strip() for x in value if str(x).strip()]
        if not value:
            return []
        text = str(value)
        if ";" in text:
            parts = text.split(";")
        else:
            parts = text.split(",")
        return [p.strip() for p in parts if p.strip()]

    journal = raw.get("Journal") or raw.get("journal") or raw.get("venue") or "Unknown"
    return {
        "title": raw.get("Title") or raw.get("title") or "Untitled",
        "url": raw.get("URL") or raw.get("url") or "#",
        "authors": split_people(raw.get("Authors") or raw.get("authors")),
        "institution": split_people(raw.get("Institutions") or raw.get("institution")),
        "published": raw.get("Date") or raw.get("published") or raw.get("date") or "N/A",
        "journal": journal,
        "abstract": raw.get("Abstract") or raw.get("abstract") or "",
        "doi": raw.get("DOI") or raw.get("doi") or "",
        "publisher": raw.get("Publisher") or raw.get("publisher") or "",
        "source_page": raw.get("SourcePage") or raw.get("source_page") or "",
        "citation_count": raw.get("citation_count", 0),
        "method": raw.get("method") or "N/A",
    }



def sort_records(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        items,
        key=lambda x: (
            x.get("published", ""),
            x.get("title", "").lower(),
        ),
        reverse=True,
    )



def group_by_journal(records: List[Dict[str, Any]]) -> OrderedDict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for rec in records:
        grouped.setdefault(rec["journal"], []).append(rec)

    ordered: OrderedDict[str, List[Dict[str, Any]]] = OrderedDict()
    for name in DEFAULT_JOURNAL_ORDER:
        if name in grouped:
            ordered[name] = sort_records(grouped.pop(name))
    for name in sorted(grouped):
        ordered[name] = sort_records(grouped[name])
    return ordered



def load_records_from_json(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [normalize_record(x) for x in data if isinstance(x, dict)]
    if isinstance(data, dict) and isinstance(data.get("journals"), dict):
        rows: List[Dict[str, Any]] = []
        for journal_name, items in data["journals"].items():
            if not isinstance(items, list):
                continue
            for item in items:
                if isinstance(item, dict):
                    row = normalize_record(item)
                    row["journal"] = journal_name or row["journal"]
                    rows.append(row)
        return rows
    raise ValueError(f"Unsupported JSON structure in {path}")



def render_item(item: Dict[str, Any]) -> str:
    authors = ", ".join(item.get("authors", [])) or "N/A"
    institutions = ", ".join(item.get("institution", [])) or "N/A"
    journal = item.get("journal", "") or "N/A"
    abstract_text = item.get("abstract", "") or "Abstract not available."
    doi = item.get("doi", "")
    publisher = item.get("publisher", "")

    doi_html = f'<p><strong>DOI:</strong> {html_escape(doi)}</p>' if doi else ""
    publisher_html = f'<p><strong>Publisher:</strong> {html_escape(publisher)}</p>' if publisher else ""

    return f"""
    <article class=\"card\">
      <h4><a href=\"{html_escape(item['url'])}\" target=\"_blank\" rel=\"noopener noreferrer\">{html_escape(item['title'])}</a></h4>
      <p><strong>Authors:</strong> {html_escape(authors)}</p>
      <p><strong>Institution:</strong> {html_escape(institutions)}</p>
      <p><strong>Published:</strong> {html_escape(item.get('published', 'N/A'))}</p>
      <p><strong>Journal:</strong> {html_escape(journal)}</p>
      {publisher_html}
      {doi_html}
      <div>
        <strong>Abstract:</strong>
        <p>{html_escape(abstract_text)}</p>
      </div>
    </article>
    """



def render_journal_section(journal_name: str, items: List[Dict[str, Any]]) -> str:
    anchor = journal_name.lower().replace(" ", "-").replace("&", "and").replace(",", "")
    body = '<p class="empty">No items in this section.</p>' if not items else "\n".join(render_item(item) for item in items)
    return f"""
    <section id=\"{anchor}\" class=\"journal-section\">
      <h2>{html_escape(journal_name)} <span class=\"count\">({len(items)})</span></h2>
      {body}
    </section>
    """



def render_page(title: str, page_heading: str, date_str: str, journals: OrderedDict[str, List[Dict[str, Any]]], archive_links: List[str], home_href: str) -> str:
    nav_links = "".join(
        f'<a class="tab" href="#{journal.lower().replace(" ", "-").replace("&", "and").replace(",", "")}">{html_escape(journal)}</a>'
        for journal in journals.keys()
    )
    journal_sections = "\n".join(render_journal_section(journal, items) for journal, items in journals.items())
    archive_html = "".join(f'<li><a href="./archive/{d}.html">{d}</a></li>' for d in archive_links) or "<li>No archive yet.</li>"

    total_papers = sum(len(v) for v in journals.values())

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
  <title>{html_escape(title)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; max-width: 1120px; margin: 0 auto; padding: 24px; line-height: 1.6; color: #222; background: #fafafa; }}
    .topbar {{ margin-bottom: 20px; }}
    .tabs {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 16px 0 28px; }}
    .tab {{ padding: 8px 12px; border: 1px solid #ccc; border-radius: 999px; text-decoration: none; color: #222; background: #f7f7f7; }}
    .tab:hover {{ background: #ececec; }}
    .journal-section {{ margin-bottom: 46px; }}
    .card {{ border: 1px solid #ddd; border-radius: 10px; padding: 16px; margin: 14px 0; background: #fff; box-shadow: 0 3px 10px rgba(0,0,0,.04); }}
    .card h4 {{ margin-top: 0; margin-bottom: 8px; font-size: 1.05rem; }}
    .archive {{ margin-top: 50px; padding-top: 16px; border-top: 2px solid #eee; }}
    .empty {{ color: #666; font-style: italic; }}
    .count {{ color: #666; font-size: .95rem; font-weight: normal; }}
    a {{ color: #0a58ca; }}
    .summary {{ color: #555; margin-bottom: 8px; }}
  </style>
</head>
<body>
  <div class=\"topbar\">
    <h1>{html_escape(page_heading)}</h1>
    <p><strong>Date:</strong> {html_escape(date_str)}</p>
    <p class=\"summary\"><strong>Total papers:</strong> {total_papers}</p>
    <p><a href=\"{html_escape(home_href)}\">Home</a></p>
  </div>

  <div class=\"tabs\">
    {nav_links}
  </div>

  {journal_sections}

  <div class=\"archive\">
    <h2>Previous Updates</h2>
    <ul>
      {archive_html}
    </ul>
  </div>
</body>
</html>
"""



def render_archive_index(archive_dates: List[str]) -> str:
    items = "".join(f'<li><a href="./{d}.html">{d}</a></li>' for d in archive_dates) or "<li>No archive yet.</li>"
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
  <title>Research Archive</title>
</head>
<body>
  <h1>Research Archive</h1>
  <p><a href=\"../index.html\">Back to Research</a></p>
  <ul>{items}</ul>
</body>
</html>
"""



def save_daily_payload(records: List[Dict[str, Any]], out_json: Path, date_str: str) -> OrderedDict[str, List[Dict[str, Any]]]:
    journals = group_by_journal(records)
    payload = {"date": date_str, "journals": journals}
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return journals



def main() -> None:
    parser = argparse.ArgumentParser(description="Build research pages from fetched JSON.")
    parser.add_argument("--input", type=Path, default=Path("output/research_articles.json"), help="Input JSON path")
    parser.add_argument("--output", type=Path, default=None, help="Standalone output HTML path")
    parser.add_argument("--daily-json", type=Path, default=None, help="Optional daily grouped JSON output path")
    parser.add_argument("--date", default=None, help="Date string for page title and daily JSON")
    parser.add_argument("--title", default="Research Digest", help="Page title")
    args = parser.parse_args()

    records = load_records_from_json(args.input)
    date_str = args.date or (today_str() if today_str else date.today().isoformat())

    if args.output is not None:
        journals = group_by_journal(records)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            render_page(
                title=args.title,
                page_heading=args.title,
                date_str=date_str,
                journals=journals,
                archive_links=[],
                home_href="#",
            ),
            encoding="utf-8",
        )
        print(f"[DONE] Wrote {args.output}")
        return

    if ensure_dir is None:
        raise SystemExit("Project utils not found. Use --output for standalone mode, or run this inside your project.")

    ensure_dir(RESEARCH_DIR)
    ensure_dir(ARCHIVE_DIR)
    ensure_dir(DATA_DIR)

    today_json = args.daily_json or (DATA_DIR / f"{date_str}.json")
    journals = save_daily_payload(records, today_json, date_str)

    archive_dates = sorted([p.stem for p in DATA_DIR.glob("*.json") if p.stem != date_str], reverse=True)

    research_index = render_page(
        title=f"Research Digest {date_str}",
        page_heading="Research Digest",
        date_str=date_str,
        journals=journals,
        archive_links=archive_dates[:30],
        home_href="../index.html",
    )
    archive_page = render_page(
        title=f"Research Archive {date_str}",
        page_heading="Research Digest Archive",
        date_str=date_str,
        journals=journals,
        archive_links=archive_dates[:30],
        home_href="../index.html",
    )

    (RESEARCH_DIR / "index.html").write_text(research_index, encoding="utf-8")
    (ARCHIVE_DIR / f"{date_str}.html").write_text(archive_page, encoding="utf-8")
    (ARCHIVE_DIR / "index.html").write_text(render_archive_index([date_str] + archive_dates[:59]), encoding="utf-8")

    print(f"[DONE] Wrote {today_json}")
    print(f"[DONE] Wrote {RESEARCH_DIR / 'index.html'}")
    print(f"[DONE] Wrote {ARCHIVE_DIR / f'{date_str}.html'}")


if __name__ == "__main__":
    main()
