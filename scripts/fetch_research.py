from pathlib import Path
from datetime import datetime, timezone, timedelta
import urllib.parse
import urllib.request
import json
import os

from utils import (
    ROOT,
    ensure_dir,
    load_json,
    save_json,
    normalize_title,
    clean_text,
    today_str,
)

DATA_DIR = ROOT / "data" / "research"
JOURNALS_FILE = DATA_DIR / "journals.json"
SEEN_FILE = DATA_DIR / "seen.json"
DAILY_DIR = DATA_DIR / "daily"

SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY", "").strip()

CURRENT_YEAR = datetime.now(timezone.utc).year
LOOKBACK_YEARS = 8

NEW_PER_JOURNAL = 5
CITED_PER_JOURNAL = 5

CROSSREF_ROWS_PER_JOURNAL = 120


def normalized_journal_name(name: str) -> str:
    return clean_text(name).lower()


def html_strip(text: str) -> str:
    if not text:
        return ""
    import re
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def safe_json_request(url: str, headers=None, timeout=30):
    req = urllib.request.Request(
        url,
        headers=headers or {"User-Agent": "Daily-Intelligence-Hub/1.0"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_crossref_date(item: dict) -> str:
    for key in ["published-online", "published-print", "issued", "created"]:
        block = item.get(key)
        if not block:
            continue
        date_parts = block.get("date-parts", [])
        if not date_parts or not date_parts[0]:
            continue
        parts = date_parts[0]
        year = parts[0]
        month = parts[1] if len(parts) > 1 else 1
        day = parts[2] if len(parts) > 2 else 1
        try:
            return f"{year:04d}-{month:02d}-{day:02d}"
        except Exception:
            continue
    return ""


def query_crossref_for_journal(journal_name: str, rows: int = CROSSREF_ROWS_PER_JOURNAL):
    from_date = (
        datetime.now(timezone.utc) - timedelta(days=365 * LOOKBACK_YEARS)
    ).strftime("%Y-%m-%d")

    encoded_journal = urllib.parse.quote(journal_name)

    url = (
        "https://api.crossref.org/works?"
        f"query.container-title={encoded_journal}"
        f"&filter=from-pub-date:{from_date},type:journal-article"
        f"&rows={rows}"
        "&sort=published"
        "&order=desc"
    )

    try:
        data = safe_json_request(url, timeout=30)
        return data.get("message", {}).get("items", [])
    except Exception as e:
        print(f"Crossref query failed for journal '{journal_name}': {e}")
        return []


def parse_crossref_item(item: dict):
    title_list = item.get("title", []) or []
    title = clean_text(title_list[0]) if title_list else ""

    abstract = html_strip(item.get("abstract", "") or "")
    doi = clean_text(item.get("DOI", "") or "")
    url = clean_text(item.get("URL", "") or "")
    journal = clean_text((item.get("container-title", []) or [""])[0])

    authors = []
    institutions = []

    for a in item.get("author", []) or []:
        given = clean_text(a.get("given", ""))
        family = clean_text(a.get("family", ""))
        full_name = clean_text(f"{given} {family}")
        if full_name:
            authors.append(full_name)

        for aff in a.get("affiliation", []) or []:
            aff_name = clean_text(aff.get("name", ""))
            if aff_name and aff_name not in institutions:
                institutions.append(aff_name)

    published = parse_crossref_date(item)

    item_id = f"doi:{doi.lower()}" if doi else f"url:{url}"

    return {
        "id": item_id,
        "doi": doi,
        "title": title,
        "abstract_raw": abstract,
        "published": published,
        "url": url,
        "authors": authors,
        "institutions": institutions[:6],
        "journal": journal
    }


def year_of(date_str: str) -> int:
    if not date_str:
        return 0
    try:
        return int(date_str[:4])
    except Exception:
        return 0


def citation_sort_key(item):
    return (
        item.get("citation_count", 0),
        item.get("published", "")
    )


def normalize_spaces(s: str) -> str:
    return " ".join(clean_text(s).lower().split())


def titles_match(a: str, b: str) -> bool:
    na = normalize_spaces(a)
    nb = normalize_spaces(b)
    if not na or not nb:
        return False
    return na == nb or na in nb or nb in na


def query_semantic_scholar_by_doi(doi: str):
    if not doi:
        return None

    encoded_doi = urllib.parse.quote(f"DOI:{doi}")
    fields = ",".join([
        "title",
        "citationCount",
        "venue",
        "authors",
        "authors.affiliations"
    ])
    url = f"https://api.semanticscholar.org/graph/v1/paper/{encoded_doi}?fields={urllib.parse.quote(fields)}"

    headers = {}
    if SEMANTIC_SCHOLAR_API_KEY:
        headers["x-api-key"] = SEMANTIC_SCHOLAR_API_KEY

    try:
        return safe_json_request(url, headers=headers, timeout=30)
    except Exception:
        return None


def query_semantic_scholar_by_title(title: str):
    encoded = urllib.parse.quote(title)
    fields = ",".join([
        "title",
        "citationCount",
        "venue",
        "authors",
        "authors.affiliations"
    ])
    url = (
        "https://api.semanticscholar.org/graph/v1/paper/search?"
        f"query={encoded}&limit=3&fields={urllib.parse.quote(fields)}"
    )

    headers = {}
    if SEMANTIC_SCHOLAR_API_KEY:
        headers["x-api-key"] = SEMANTIC_SCHOLAR_API_KEY

    try:
        data = safe_json_request(url, headers=headers, timeout=30)
        papers = data.get("data", [])
        if not papers:
            return None

        for p in papers:
            if titles_match(title, p.get("title", "")):
                return p

        return papers[0]
    except Exception:
        return None


def extract_institutions_from_s2(paper_obj):
    institutions = []
    if not paper_obj:
        return institutions

    for author in paper_obj.get("authors", []) or []:
        for aff in author.get("affiliations", []) or []:
            aff_clean = clean_text(str(aff))
            if aff_clean and aff_clean not in institutions:
                institutions.append(aff_clean)
    return institutions[:6]


def enrich_with_semantic_scholar(item):
    s2 = None

    if item.get("doi"):
        s2 = query_semantic_scholar_by_doi(item["doi"])

    if not s2:
        s2 = query_semantic_scholar_by_title(item.get("title", ""))

    if not s2:
        return {
            "citation_count": 0,
            "institutions": [],
            "venue": ""
        }

    return {
        "citation_count": int(s2.get("citationCount", 0) or 0),
        "institutions": extract_institutions_from_s2(s2),
        "venue": clean_text(str(s2.get("venue", "")))
    }


def build_final_item(raw_item):
    institutions = raw_item.get("institutions", []) or []
    venue = raw_item.get("journal", "")
    citation_count = 0

    enriched = enrich_with_semantic_scholar(raw_item)
    citation_count = enriched.get("citation_count", 0)

    if not institutions:
        institutions = enriched.get("institutions", []) or []

    if enriched.get("venue"):
        venue = enriched["venue"]

    if not institutions:
        institutions = ["Not available from source"]

    abstract_text = clean_text(raw_item.get("abstract_raw", ""))
    if not abstract_text:
        abstract_text = "Abstract not available."

    combined_text = " ".join([
        raw_item.get("title", ""),
        raw_item.get("abstract_raw", ""),
        raw_item.get("journal", "")
    ]).lower()

    method = classify_method(combined_text)

    return {
        "id": raw_item["id"],
        "title": raw_item["title"],
        "authors": raw_item.get("authors", [])[:8],
        "institution": institutions[:6],
        "published": raw_item.get("published", ""),
        "abstract": abstract_text,
        "url": raw_item.get("url", ""),
        "venue": venue,
        "citation_count": citation_count,
        "method": method
    }


def classify_method(text: str):
    experimental_keys = [
        "experiment", "experimental", "laboratory", "specimen", "measured",
        "measurement", "field test", "field experiment", "sensor", "testbed"
    ]
    numerical_keys = [
        "simulation", "numerical", "finite element", "finite-element",
        "fem", "modeling", "modelling", "comsol", "abaqus"
    ]
    ml_keys = [
        "machine learning", "deep learning", "neural network", "cnn", "rnn",
        "transformer", "random forest", "svm", "support vector machine",
        "xgboost", "artificial intelligence"
    ]
    theory_keys = [
        "analytical", "theoretical", "closed-form", "derivation", "formula",
        "mathematical model", "theory"
    ]
    review_keys = [
        "review", "survey", "overview", "bibliometric", "state of the art"
    ]

    exp_hit = any(k in text for k in experimental_keys)
    num_hit = any(k in text for k in numerical_keys)
    ml_hit = any(k in text for k in ml_keys)
    theory_hit = any(k in text for k in theory_keys)
    review_hit = any(k in text for k in review_keys)

    if review_hit:
        return "Review / Survey"

    count = sum([exp_hit, num_hit, ml_hit, theory_hit])

    if count >= 2:
        return "Hybrid"
    if ml_hit:
        return "Machine Learning"
    if num_hit:
        return "Numerical Simulation"
    if exp_hit:
        return "Experimental"
    if theory_hit:
        return "Analytical / Theoretical"
    return "Other"


def main():
    ensure_dir(DAILY_DIR)

    journals = load_json(JOURNALS_FILE, [])
    seen = load_json(SEEN_FILE, {"featured_ids": [], "featured_titles": []})

    selected_journals = {normalized_journal_name(j) for j in journals}
    featured_ids = set(seen.get("featured_ids", []))
    featured_titles = set(seen.get("featured_titles", []))

    results_by_journal = {}
    newly_featured_ids = []
    newly_featured_titles = []

    for journal_name in journals:
        items = query_crossref_for_journal(journal_name, rows=CROSSREF_ROWS_PER_JOURNAL)

        parsed_items = []
        for raw in items:
            parsed = parse_crossref_item(raw)

            if not parsed["title"] or not parsed["url"] or not parsed["published"]:
                continue

            if normalized_journal_name(parsed["journal"]) not in selected_journals:
                continue

            norm_title = normalize_title(parsed["title"])
            if parsed["id"] in featured_ids or norm_title in featured_titles:
                continue

            parsed_items.append(parsed)

        # local dedup
        deduped = []
        local_ids = set()
        local_titles = set()
        for item in parsed_items:
            norm_title = normalize_title(item["title"])
            if item["id"] in local_ids or norm_title in local_titles:
                continue
            local_ids.add(item["id"])
            local_titles.add(norm_title)
            deduped.append(item)

        # enrich once
        enriched_items = [build_final_item(x) for x in deduped]

        new_items = sorted(
            [x for x in enriched_items if year_of(x["published"]) == CURRENT_YEAR],
            key=lambda x: x["published"],
            reverse=True
        )[:NEW_PER_JOURNAL]

        selected_ids = {x["id"] for x in new_items}

        cited_candidates = [
            x for x in enriched_items
            if year_of(x["published"]) < CURRENT_YEAR and x["id"] not in selected_ids
        ]
        cited_items = sorted(
            cited_candidates,
            key=citation_sort_key,
            reverse=True
        )[:CITED_PER_JOURNAL]

        final_items = {
            "new": new_items,
            "cited": cited_items
        }
        results_by_journal[journal_name] = final_items

        for bucket in ["new", "cited"]:
            for item in final_items[bucket]:
                norm_title = normalize_title(item["title"])
                newly_featured_ids.append(item["id"])
                newly_featured_titles.append(norm_title)

    all_empty = True
    for payload in results_by_journal.values():
        if payload.get("new") or payload.get("cited"):
            all_empty = False
            break

    if all_empty:
        print("No new journal items found. Falling back to the latest previous non-empty daily file.")
        existing_files = sorted(DAILY_DIR.glob("*.json"), reverse=True)
        for f in existing_files:
            if f.stem == today_str():
                continue
            prev = load_json(f, {})
            prev_journals = prev.get("journals", {})
            has_any = False
            for payload in prev_journals.values():
                if payload.get("new") or payload.get("cited"):
                    has_any = True
                    break
            if has_any:
                results_by_journal = prev_journals
                break
    else:
        seen["featured_ids"] = sorted(list(set(featured_ids.union(newly_featured_ids))))
        seen["featured_titles"] = sorted(list(set(featured_titles.union(newly_featured_titles))))
        save_json(SEEN_FILE, seen)

    out = {
        "date": today_str(),
        "journals": results_by_journal
    }

    out_file = DAILY_DIR / f"{today_str()}.json"
    save_json(out_file, out)
    print(f"Saved research data to {out_file}")


if __name__ == "__main__":
    main()
