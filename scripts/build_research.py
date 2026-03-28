
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_MS = 60000
DEFAULT_WAIT_MS = 2500


def try_import_project_utils():
    try:
        from utils import ROOT, ensure_dir, today_str  # type: ignore
        return ROOT, ensure_dir, today_str
    except Exception:
        return None, None, None


PROJECT_ROOT, PROJECT_ENSURE_DIR, PROJECT_TODAY_STR = try_import_project_utils()


@dataclass(frozen=True)
class JournalConfig:
    code: str
    journal: str
    publisher: str
    listing_urls: List[str]
    start_urls: List[str]
    aliases: List[str]


JOURNALS: List[JournalConfig] = [
    JournalConfig(
        code="MSSP",
        journal="Mechanical Systems and Signal Processing",
        publisher="elsevier",
        listing_urls=[
            "https://www.sciencedirect.com/journal/mechanical-systems-and-signal-processing/articles-in-press",
            "https://www.sciencedirect.com/journal/mechanical-systems-and-signal-processing",
            "https://www.sciencedirect.com/journal/mechanical-systems-and-signal-processing/issues",
        ],
        start_urls=[
            "https://www.sciencedirect.com/journal/mechanical-systems-and-signal-processing/articles-in-press",
        ],
        aliases=["mechanical systems and signal processing", "mssp"],
    ),
    JournalConfig(
        code="TUST",
        journal="Tunnelling and Underground Space Technology",
        publisher="elsevier",
        listing_urls=[
            "https://www.sciencedirect.com/journal/tunnelling-and-underground-space-technology/articles-in-press",
            "https://www.sciencedirect.com/journal/tunnelling-and-underground-space-technology",
            "https://www.sciencedirect.com/journal/tunnelling-and-underground-space-technology/issues",
        ],
        start_urls=[
            "https://www.sciencedirect.com/journal/tunnelling-and-underground-space-technology/articles-in-press",
        ],
        aliases=[
            "tunnelling and underground space technology",
            "tunneling and underground space technology",
            "tust",
        ],
    ),
    JournalConfig(
        code="ES",
        journal="Engineering Structures",
        publisher="elsevier",
        listing_urls=[
            "https://www.sciencedirect.com/journal/engineering-structures/articles-in-press",
            "https://www.sciencedirect.com/journal/engineering-structures",
            "https://www.sciencedirect.com/journal/engineering-structures/issues",
        ],
        start_urls=[
            "https://www.sciencedirect.com/journal/engineering-structures/articles-in-press",
        ],
        aliases=["engineering structures", "es"],
    ),
    JournalConfig(
        code="MEAS",
        journal="Measurement",
        publisher="elsevier",
        listing_urls=[
            "https://www.sciencedirect.com/journal/measurement/articles-in-press",
            "https://www.sciencedirect.com/journal/measurement",
            "https://www.sciencedirect.com/journal/measurement/issues",
        ],
        start_urls=[
            "https://www.sciencedirect.com/journal/measurement/articles-in-press",
        ],
        aliases=["measurement", "meas"],
    ),
    JournalConfig(
        code="NDTE",
        journal="NDT & E International",
        publisher="elsevier",
        listing_urls=[
            "https://www.sciencedirect.com/journal/ndt-and-e-international/articles-in-press",
            "https://www.sciencedirect.com/journal/ndt-and-e-international",
            "https://www.sciencedirect.com/journal/ndt-and-e-international/issues",
        ],
        start_urls=[
            "https://www.sciencedirect.com/journal/ndt-and-e-international/articles-in-press",
        ],
        aliases=["ndt & e international", "ndt and e international", "ndte"],
    ),
    JournalConfig(
        code="ESWA",
        journal="Expert Systems with Applications",
        publisher="elsevier",
        listing_urls=[
            "https://www.sciencedirect.com/journal/expert-systems-with-applications/articles-in-press",
            "https://www.sciencedirect.com/journal/expert-systems-with-applications",
            "https://www.sciencedirect.com/journal/expert-systems-with-applications/issues",
        ],
        start_urls=[
            "https://www.sciencedirect.com/journal/expert-systems-with-applications/articles-in-press",
        ],
        aliases=["expert systems with applications", "eswa"],
    ),
    JournalConfig(
        code="RMRE",
        journal="Rock Mechanics and Rock Engineering",
        publisher="springer",
        listing_urls=[
            "https://link.springer.com/journal/603/articles",
            "https://link.springer.com/journal/603/online-first-articles",
            "https://link.springer.com/journal/603/volumes-and-issues",
        ],
        start_urls=[
            "https://link.springer.com/journal/603/articles",
        ],
        aliases=["rock mechanics and rock engineering", "rmre", "rock mech rock eng"],
    ),
]


@dataclass
class PaperRecord:
    Journal: str
    Title: str
    DOI: str
    URL: str
    Publisher: str
    Date: str
    Year: Optional[int]
    Authors: str
    Institutions: str
    Abstract: str
    SourcePage: str
    FetchTimestampUTC: str


def utc_now_iso() -> str:
    return pd.Timestamp.utcnow().isoformat()


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def normalize_journal_name(text: str) -> str:
    t = normalize_space(text).lower()
    t = t.replace("&", "and")
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return normalize_space(t)


def parse_date_safe(text: str) -> Tuple[str, Optional[int]]:
    raw = normalize_space(text)
    if not raw:
        return "", None
    try:
        dt = dateparser.parse(raw, fuzzy=True)
        return dt.date().isoformat(), dt.year
    except Exception:
        return raw, None


def unique_keep_order(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        x = normalize_space(item)
        if not x:
            continue
        k = x.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def text_or_empty(node) -> str:
    return normalize_space(node.get_text(" ", strip=True)) if node else ""


def meta_all(soup: BeautifulSoup, names: Sequence[str]) -> List[str]:
    values: List[str] = []
    for tag in soup.find_all("meta"):
        key = (tag.get("name") or tag.get("property") or tag.get("itemprop") or "").strip().lower()
        if key in {n.lower() for n in names}:
            content = normalize_space(tag.get("content", ""))
            if content:
                values.append(content)
    return unique_keep_order(values)


def meta_first(soup: BeautifulSoup, names: Sequence[str]) -> str:
    vals = meta_all(soup, names)
    return vals[0] if vals else ""


def select_journals(codes: Optional[List[str]]) -> List[JournalConfig]:
    if not codes:
        return JOURNALS
    wanted = {c.strip().upper() for c in codes}
    out = [j for j in JOURNALS if j.code.upper() in wanted]
    if not out:
        raise SystemExit(f"No journals matched --journals {codes}")
    return out


def strict_journal_match(found: str, cfg: JournalConfig) -> bool:
    nf = normalize_journal_name(found)
    allowed = {normalize_journal_name(cfg.journal), *[normalize_journal_name(a) for a in cfg.aliases]}
    return nf in allowed


def is_article_url(url: str, publisher: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path
    if publisher == "elsevier":
        return "sciencedirect.com" in host and "/science/article/pii/" in path
    if publisher == "springer":
        return "link.springer.com" in host and "/article/" in path
    return False


def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def collect_article_urls_from_listing(page, listing_url: str, publisher: str) -> List[str]:
    page.goto(listing_url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
    page.wait_for_timeout(DEFAULT_WAIT_MS)
    try:
        page.locator("button:has-text('Accept all')").click(timeout=3000)
    except Exception:
        pass
    try:
        page.locator("button:has-text('Accept')").click(timeout=2000)
    except Exception:
        pass
    page.wait_for_timeout(1000)
    html = page.content()
    soup = BeautifulSoup(html, "lxml")

    urls: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        abs_url = urljoin(listing_url, href)
        abs_url = canonicalize_url(abs_url)
        if is_article_url(abs_url, publisher):
            urls.append(abs_url)
    return unique_keep_order(urls)


def parse_json_ld_candidates(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for tag in soup.find_all("script", attrs={"type": re.compile("application/ld\\+json", re.I)}):
        raw = tag.string or tag.get_text("\n", strip=True)
        raw = raw.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                out.extend([x for x in parsed if isinstance(x, dict)])
            elif isinstance(parsed, dict):
                out.append(parsed)
        except Exception:
            continue
    return out


def extract_from_json_ld(soup: BeautifulSoup) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for item in parse_json_ld_candidates(soup):
        t = str(item.get("@type", "")).lower()
        if "scholarlyarticle" in t or "article" in t:
            data.setdefault("title", item.get("headline") or item.get("name") or "")
            journal = ""
            part = item.get("isPartOf")
            if isinstance(part, dict):
                journal = part.get("name") or ""
            data.setdefault("journal", journal)
            data.setdefault("abstract", item.get("description") or item.get("abstract") or "")
            data.setdefault("date", item.get("datePublished") or "")
            data.setdefault("doi", item.get("identifier") or "")
            authors: List[str] = []
            author_obj = item.get("author", [])
            if isinstance(author_obj, dict):
                author_obj = [author_obj]
            if isinstance(author_obj, list):
                for a in author_obj:
                    if isinstance(a, dict):
                        name = a.get("name") or ""
                        if name:
                            authors.append(name)
            if authors:
                data.setdefault("authors", authors)
    return data


def extract_abstract_generic(soup: BeautifulSoup) -> str:
    selectors = [
        "section.Abstract",
        "div.abstract.author",
        "div.Abstracts",
        "#Abs1-content",
        "div.c-article-section__content",
        "section#Abs1",
        "div[class*='abstract']",
    ]
    for sel in selectors:
        node = soup.select_one(sel)
        txt = text_or_empty(node)
        if txt and len(txt) > 80:
            txt = re.sub(r"^Abstract\s*", "", txt, flags=re.I)
            return txt
    # heading-based fallback
    for heading in soup.find_all(re.compile("^h[1-6]$")):
        if normalize_journal_name(heading.get_text()) == "abstract":
            bits: List[str] = []
            for sib in heading.find_next_siblings():
                txt = text_or_empty(sib)
                if not txt:
                    continue
                if sib.name and re.fullmatch(r"h[1-6]", sib.name, flags=re.I):
                    break
                bits.append(txt)
                if sum(len(x) for x in bits) > 1500:
                    break
            joined = normalize_space(" ".join(bits))
            if len(joined) > 80:
                return joined
    meta_desc = meta_first(soup, ["citation_abstract", "description", "dc.description", "og:description"])
    return meta_desc


def extract_institutions_generic(soup: BeautifulSoup) -> List[str]:
    institutions = meta_all(soup, ["citation_author_institution", "dc.contributor.affiliation"])
    if institutions:
        return institutions

    text = soup.get_text("\n", strip=True)
    lines = [normalize_space(x) for x in text.splitlines()]
    keywords = (
        "university",
        "institute",
        "school of",
        "college of",
        "department of",
        "laboratory",
        "centre",
        "center",
        "academy",
        "hospital",
        "faculty of",
    )
    guessed: List[str] = []
    for line in lines:
        low = line.lower()
        if any(k in low for k in keywords) and 8 <= len(line) <= 250:
            guessed.append(line)
    return unique_keep_order(guessed)[:10]


def scrape_article(page, url: str, cfg: JournalConfig) -> Optional[PaperRecord]:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
        page.wait_for_timeout(DEFAULT_WAIT_MS)
    except PlaywrightTimeoutError:
        print(f"[WARN] Timeout while opening article: {url}", file=sys.stderr)
        return None
    except Exception as exc:
        print(f"[WARN] Failed article load: {url} :: {exc}", file=sys.stderr)
        return None

    html = page.content()
    soup = BeautifulSoup(html, "lxml")
    jld = extract_from_json_ld(soup)

    title = (
        meta_first(soup, ["citation_title", "dc.title", "og:title"]) or
        jld.get("title", "") or
        text_or_empty(soup.select_one("h1"))
    )

    journal = (
        meta_first(soup, ["citation_journal_title", "prism.publicationname", "dc.source", "citation_journal_abbrev"]) or
        jld.get("journal", "")
    )

    if not journal:
        page_text = normalize_space(soup.get_text(" ", strip=True))
        if cfg.journal in page_text:
            journal = cfg.journal

    if not strict_journal_match(journal, cfg):
        print(
            f"[SKIP] Journal mismatch for {url} :: found='{journal}' expected='{cfg.journal}'",
            file=sys.stderr,
        )
        return None

    doi = meta_first(soup, ["citation_doi", "dc.identifier", "prism.doi"]) or str(jld.get("doi", ""))
    doi = doi.replace("https://doi.org/", "").strip()

    date_raw = (
        meta_first(
            soup,
            [
                "citation_online_date",
                "citation_publication_date",
                "prism.publicationdate",
                "dc.date",
                "article:published_time",
            ],
        )
        or str(jld.get("date", ""))
    )
    date_iso, year = parse_date_safe(date_raw)

    authors = meta_all(soup, ["citation_author", "dc.creator"])
    if not authors:
        authors = jld.get("authors", []) if isinstance(jld.get("authors"), list) else []
    if not authors:
        author_nodes = soup.select("[class*='author'] a, [class*='authors'] a")
        authors = unique_keep_order(text_or_empty(n) for n in author_nodes)

    abstract = extract_abstract_generic(soup)
    institutions = extract_institutions_generic(soup)

    return PaperRecord(
        Journal=cfg.journal,
        Title=title,
        DOI=doi,
        URL=url,
        Publisher=cfg.publisher,
        Date=date_iso,
        Year=year,
        Authors="; ".join(unique_keep_order(authors)),
        Institutions="; ".join(institutions),
        Abstract=abstract,
        SourcePage="",
        FetchTimestampUTC=utc_now_iso(),
    )


def dedupe_records(records: List[PaperRecord]) -> List[PaperRecord]:
    seen = set()
    out: List[PaperRecord] = []
    for rec in records:
        key = (rec.DOI or rec.URL or rec.Title).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(rec)
    return out


def scrape_journal(browser, cfg: JournalConfig, max_per_journal: int) -> List[PaperRecord]:
    context = browser.new_context(user_agent=USER_AGENT, locale="en-US")
    page = context.new_page()
    page.set_default_timeout(DEFAULT_TIMEOUT_MS)

    discovered: List[Tuple[str, str]] = []
    for listing_url in cfg.listing_urls:
        try:
            urls = collect_article_urls_from_listing(page, listing_url, cfg.publisher)
            for u in urls:
                discovered.append((u, listing_url))
            print(f"[INFO] {cfg.code}: discovered {len(urls)} links from {listing_url}")
        except Exception as exc:
            print(f"[WARN] Failed listing page {listing_url} :: {exc}", file=sys.stderr)

    unique_urls: List[Tuple[str, str]] = []
    seen = set()
    for u, src in discovered:
        k = canonicalize_url(u)
        if k in seen:
            continue
        seen.add(k)
        unique_urls.append((u, src))

    records: List[PaperRecord] = []
    for url, src in unique_urls:
        if len(records) >= max_per_journal:
            break
        rec = scrape_article(page, url, cfg)
        if rec is None:
            continue
        rec.SourcePage = src
        if not rec.Title:
            continue
        records.append(rec)
        print(f"[OK] {cfg.code}: {rec.Title[:100]}")

    context.close()
    return dedupe_records(records)


def save_outputs(records: List[PaperRecord], outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    rows = [asdict(r) for r in records]
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(by=["Journal", "Date", "Title"], ascending=[True, False, True], na_position="last")

    json_path = outdir / "research_articles.json"
    csv_path = outdir / "research_articles.csv"
    md_path = outdir / "research_articles.md"

    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    lines = ["# Research Articles", ""]
    grouped = {}
    for rec in rows:
        grouped.setdefault(rec["Journal"], []).append(rec)

    for journal in sorted(grouped):
        lines.append(f"## {journal}")
        lines.append("")
        for r in grouped[journal]:
            lines.append(f"### {r['Title']}")
            lines.append("")
            lines.append(f"- **Journal:** {r['Journal']}")
            lines.append(f"- **Date:** {r['Date']}")
            lines.append(f"- **DOI:** {r['DOI']}")
            lines.append(f"- **URL:** {r['URL']}")
            lines.append(f"- **Authors:** {r['Authors']}")
            lines.append(f"- **Institutions:** {r['Institutions']}")
            lines.append("")
            lines.append(r["Abstract"] or "")
            lines.append("")
        lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"[DONE] Wrote {json_path}")
    print(f"[DONE] Wrote {csv_path}")
    print(f"[DONE] Wrote {md_path}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch recent papers from official journal websites only.")
    p.add_argument("--outdir", type=Path, default=Path("output"), help="Output directory")
    p.add_argument("--max-per-journal", type=int, default=25, help="Max saved papers per journal")
    p.add_argument(
        "--journals",
        nargs="*",
        default=None,
        help="Journal codes, e.g. MSSP ES TUST RMRE ESWA MEAS NDTE",
    )
    p.add_argument(
        "--show-journals",
        action="store_true",
        help="Print supported journal codes and exit",
    )
    return p


def main() -> None:
    args = build_arg_parser().parse_args()

    if args.show_journals:
        for j in JOURNALS:
            print(f"{j.code:5s}  {j.journal}")
        return

    selected = select_journals(args.journals)
    all_records: List[PaperRecord] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            for cfg in selected:
                print(f"\n=== Fetching {cfg.code}: {cfg.journal} ===")
                recs = scrape_journal(browser, cfg, args.max_per_journal)
                all_records.extend(recs)
                time.sleep(1)
        finally:
            browser.close()

    all_records = dedupe_records(all_records)
    save_outputs(all_records, args.outdir, write_daily_json=True)


if __name__ == "__main__":
    main()
