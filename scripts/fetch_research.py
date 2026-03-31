from pathlib import Path
from datetime import datetime, timezone
import urllib.parse
import urllib.request
import json
import os
import time
import re
import html

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

OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY", "").strip()
CROSSREF_MAILTO = os.getenv("CROSSREF_MAILTO", "").strip()
SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY", "").strip()

CURRENT_YEAR = datetime.now(timezone.utc).year

NEW_PER_JOURNAL = 5
CITED_PER_JOURNAL = 5
MAX_PER_JOURNAL = 10

OPENALEX_PER_PAGE = 100
OPENALEX_MAX_PAGES = 3  # 300 works max per journal


def normalized_text(text: str) -> str:
    return clean_text(text).lower()


def safe_json_request(url: str, headers=None, timeout=30):
    req = urllib.request.Request(
        url,
        headers=headers or {"User-Agent": "Daily-Intelligence-Hub/1.0"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def normalize_spaces(s: str) -> str:
    return " ".join(clean_text(s).lower().split())


def titles_match(a: str, b: str) -> bool:
    na = normalize_spaces(a)
    nb = normalize_spaces(b)
    if not na or not nb:
        return False
    return na == nb or na in nb or nb in na


def reconstruct_abstract(inv_idx):
    if not inv_idx or not isinstance(inv_idx, dict):
        return ""

    positions = {}
    for word, pos_list in inv_idx.items():
        if not isinstance(pos_list, list):
            continue
        for pos in pos_list:
            if isinstance(pos, int):
                positions[pos] = word

    if not positions:
        return ""

    words = [positions[i] for i in sorted(positions.keys())]
    return " ".join(words)


def year_of(date_str: str) -> int:
    if not date_str:
        return 0
    try:
        return int(str(date_str)[:4])
    except Exception:
        return 0


def citation_sort_key(item):
    return (
        item.get("citation_count", 0),
        item.get("published", "")
    )


def query_openalex_source_by_issn(issn: str):
    params = {
        "api_key": OPENALEX_API_KEY
    } if OPENALEX_API_KEY else {}

    query = urllib.parse.urlencode(params)
    url = f"https://api.openalex.org/sources/issn:{urllib.parse.quote(issn)}"
    if query:
        url += f"?{query}"

    try:
        data = safe_json_request(url, timeout=30)
        return data
    except Exception:
        return None


def query_openalex_works_by_source_id(source_id: str, page: int = 1):
    filter_value = f"primary_location.source.id:{source_id},type:article"
    params = {
        "filter": filter_value,
        "sort": "publication_date:desc",
        "per_page": OPENALEX_PER_PAGE,
        "page": page
    }
    if OPENALEX_API_KEY:
        params["api_key"] = OPENALEX_API_KEY

    url = "https://api.openalex.org/works?" + urllib.parse.urlencode(params)
    try:
        return safe_json_request(url, timeout=30)
    except Exception:
        return None


def query_crossref_by_doi(doi: str):
    if not doi:
        return None

    url = f"https://api.crossref.org/works/{urllib.parse.quote(doi)}"
    if CROSSREF_MAILTO:
        url += f"?mailto={urllib.parse.quote(CROSSREF_MAILTO)}"

    try:
        data = safe_json_request(url, timeout=30)
        return data.get("message", {})
    except Exception:
        return None


def query_semantic_scholar_by_doi(doi: str):
    if not doi:
        return None

    encoded_doi = urllib.parse.quote(f"DOI:{doi}")
    fields = ",".join([
        "title",
        "citationCount",
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

    return institutions[:8]


def extract_crossref_abstract(message):
    if not message:
        return ""
    raw = message.get("abstract", "") or ""
    raw = re.sub(r"<[^>]+>", " ", raw)
    raw = re.sub(r"\s+", " ", raw)
    return html.unescape(raw).strip()


def parse_openalex_work(work, target_journal_name):
    doi = clean_text((work.get("doi") or "").replace("https://doi.org/", "").replace("http://doi.org/", ""))
    title = clean_text(work.get("display_name", "") or "")
    published = clean_text(work.get("publication_date", "") or "")
    journal = target_journal_name

    # Landing page URL
    url = ""
    primary_location = work.get("primary_location") or {}
    if primary_location:
        url = clean_text(primary_location.get("landing_page_url", "") or primary_location.get("pdf_url", ""))

    # Authors + institutions from OpenAlex
    authors = []
    institutions = []
    for auth in work.get("authorships", []) or []:
        author_obj = auth.get("author") or {}
        author_name = clean_text(author_obj.get("display_name", ""))
        if author_name:
            authors.append(author_name)

        for inst in auth.get("institutions", []) or []:
            inst_name = clean_text(inst.get("display_name", ""))
            if inst_name and inst_name not in institutions:
                institutions.append(inst_name)

    # Abstract from OpenAlex
    abstract_text = reconstruct_abstract(work.get("abstract_inverted_index"))

    return {
        "id": clean_text(work.get("id", "")) or (f"doi:{doi.lower()}" if doi else title),
        "doi": doi,
        "title": title,
        "published": published,
        "url": url,
        "authors": authors[:10],
        "institutions": institutions[:8],
        "journal": journal,
        "abstract": clean_text(abstract_text),
        "citation_count": int(work.get("cited_by_count", 0) or 0),
    }


def classify_method(text: str):
    text = (text or "").lower()

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
        "xgboost", "artificial intelligence", "transfer learning"
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


def enrich_missing_fields(item):
    # Crossref fallback for abstract
    if item.get("doi") and not item.get("abstract"):
        cr = query_crossref_by_doi(item["doi"])
        abstract_text = extract_crossref_abstract(cr)
        if abstract_text:
            item["abstract"] = abstract_text

    # Semantic Scholar fallback for institution or citations
    if item.get("doi"):
        s2 = query_semantic_scholar_by_doi(item["doi"])
    else:
        s2 = query_semantic_scholar_by_title(item.get("title", ""))

    time.sleep(0.3)

    if s2:
        if not item.get("institutions"):
            item["institutions"] = extract_institutions_from_s2(s2)
        if not item.get("citation_count"):
            item["citation_count"] = int(s2.get("citationCount", 0) or 0)

    # Final normalize
    if not item.get("abstract"):
        item["abstract"] = "Abstract not available."

    if not item.get("institutions"):
        item["institutions"] = []

    combined_text = " ".join([
        item.get("title", ""),
        item.get("abstract", ""),
        item.get("journal", "")
    ]).lower()
    item["method"] = classify_method(combined_text)

    return item


def select_items_for_journal(items):
    new_items = sorted(
        [x for x in items if year_of(x["published"]) == CURRENT_YEAR],
        key=lambda x: x["published"],
        reverse=True
    )[:NEW_PER_JOURNAL]

    selected_ids = {x["id"] for x in new_items}

    cited_candidates = [
        x for x in items
        if year_of(x["published"]) < CURRENT_YEAR and x["id"] not in selected_ids
    ]
    cited_items = sorted(
        cited_candidates,
        key=citation_sort_key,
        reverse=True
    )[:CITED_PER_JOURNAL]

    selected_ids.update(x["id"] for x in cited_items)

    filler_candidates = [
        x for x in items if x["id"] not in selected_ids
    ]
    filler_items = sorted(
        filler_candidates,
        key=lambda x: x.get("published", ""),
        reverse=True
    )

    combined_items = new_items + cited_items
    for item in filler_items:
        if len(combined_items) >= MAX_PER_JOURNAL:
            break
        combined_items.append(item)

    return combined_items[:MAX_PER_JOURNAL]


def main():
    ensure_dir(DAILY_DIR)

    journals = load_json(JOURNALS_FILE, [])
    seen = load_json(SEEN_FILE, {"featured_ids": [], "featured_titles": []})

    featured_ids = set(seen.get("featured_ids", []))
    featured_titles = set(seen.get("featured_titles", []))

    results_by_journal = {}
    newly_featured_ids = []
    newly_featured_titles = []

    for journal_cfg in journals:
        target_name = journal_cfg["name"]
        issns = journal_cfg["issns"]

        source_id = None
        for issn in issns:
            source = query_openalex_source_by_issn(issn)
            if source and source.get("id"):
                source_id = source["id"]
                break

        if not source_id:
            results_by_journal[target_name] = []
            continue

        works = []
        for page in range(1, OPENALEX_MAX_PAGES + 1):
            data = query_openalex_works_by_source_id(source_id, page=page)
            if not data:
                break
            page_results = data.get("results", [])
            if not page_results:
                break
            works.extend(page_results)

        parsed_items = []
        for work in works:
            parsed = parse_openalex_work(work, target_name)

            if not parsed["title"] or not parsed["published"]:
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

        enriched_items = [enrich_missing_fields(x) for x in deduped]
        final_items = select_items_for_journal(enriched_items)

        results_by_journal[target_name] = final_items

        for item in final_items:
            norm_title = normalize_title(item["title"])
            newly_featured_ids.append(item["id"])
            newly_featured_titles.append(norm_title)

    all_empty = all(len(items) == 0 for items in results_by_journal.values())

    if all_empty:
        print("No new journal items found. Falling back to the latest previous non-empty daily file.")
        existing_files = sorted(DAILY_DIR.glob("*.json"), reverse=True)
        for f in existing_files:
            if f.stem == today_str():
                continue
            prev = load_json(f, {})
            prev_journals = prev.get("journals", {})
            has_any = False
            for items in prev_journals.values():
                if len(items) > 0:
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
