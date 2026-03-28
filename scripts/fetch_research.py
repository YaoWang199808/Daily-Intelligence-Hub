from pathlib import Path
from datetime import datetime, timezone, timedelta
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import json
import os
import re
import time

from utils import (
    ROOT,
    ensure_dir,
    load_json,
    save_json,
    normalize_title,
    clean_text,
    today_str,
    extract_arxiv_id,
)

DATA_DIR = ROOT / "data" / "research"
TOPICS_FILE = DATA_DIR / "topics.json"
JOURNALS_FILE = DATA_DIR / "journals.json"
SEEN_FILE = DATA_DIR / "seen.json"
DAILY_DIR = DATA_DIR / "daily"

ARXIV_NS = {"a": "http://www.w3.org/2005/Atom"}

SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY", "").strip()
SEMANTIC_SCHOLAR_ENABLED = True

# ---------- Display design ----------
FRESH_DAYS = 14
BACKLOG_DAYS = 120
LOOKBACK_DAYS = 365

TARGET_COUNTS = {
    "fresh": 6,
    "backlog": 8,
    "highlights": 6
}

CROSSREF_ROWS_PER_KEYWORD = 80
ARXIV_ROWS_PER_KEYWORD = 40


def normalized_journal_name(name: str) -> str:
    return clean_text(name).lower()


def html_strip(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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


def days_since(date_str: str):
    if not date_str:
        return 10**9
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return 10**9
    return (datetime.now(timezone.utc) - dt).days


def classify_bucket(published_date: str) -> str:
    age = days_since(published_date)
    if age <= FRESH_DAYS:
        return "fresh"
    if age <= BACKLOG_DAYS:
        return "backlog"
    return "highlights"


def safe_json_request(url: str, headers=None, timeout=30):
    req = urllib.request.Request(
        url,
        headers=headers or {
            "User-Agent": "Daily-Intelligence-Hub/1.0"
        }
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def query_crossref(keyword: str, rows: int = CROSSREF_ROWS_PER_KEYWORD):
    from_date = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    encoded_kw = urllib.parse.quote(keyword)

    url = (
        "https://api.crossref.org/works?"
        f"query.bibliographic={encoded_kw}"
        f"&filter=from-pub-date:{from_date}"
        f"&rows={rows}"
        "&sort=published"
        "&order=desc"
    )

    try:
        data = safe_json_request(url, timeout=30)
        return data.get("message", {}).get("items", [])
    except Exception as e:
        print(f"Crossref query failed for '{keyword}': {e}")
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
    subject = [clean_text(s) for s in (item.get("subject", []) or []) if clean_text(s)]

    item_id = f"doi:{doi.lower()}" if doi else f"url:{url}"

    return {
        "id": item_id,
        "title": title,
        "summary_raw": abstract,
        "published": published,
        "url": url,
        "authors": authors,
        "institutions": institutions[:6],
        "journal": journal,
        "categories": subject,
        "source": "Crossref"
    }


def query_arxiv(keyword: str, max_results: int = ARXIV_ROWS_PER_KEYWORD):
    query = urllib.parse.quote(f'all:"{keyword}"')
    url = (
        "http://export.arxiv.org/api/query?"
        f"search_query={query}&start=0&max_results={max_results}"
        "&sortBy=submittedDate&sortOrder=descending"
    )
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            return response.read()
    except Exception as e:
        print(f"arXiv query failed for '{keyword}': {e}")
        return b""


def parse_entries(xml_bytes: bytes):
    if not xml_bytes:
        return []
    root = ET.fromstring(xml_bytes)
    return root.findall("a:entry", ARXIV_NS)


def text_of(elem, path: str) -> str:
    found = elem.find(path, ARXIV_NS)
    return found.text.strip() if found is not None and found.text else ""


def parse_arxiv_entry(entry):
    title = clean_text(text_of(entry, "a:title"))
    summary = clean_text(text_of(entry, "a:summary"))
    published_full = text_of(entry, "a:published")
    published = published_full[:10] if published_full else ""
    link = text_of(entry, "a:id")

    authors = []
    for author in entry.findall("a:author", ARXIV_NS):
        name = text_of(author, "a:name")
        if name:
            authors.append(name)

    categories = []
    for cat in entry.findall("a:category", ARXIV_NS):
        term = cat.attrib.get("term", "").strip()
        if term:
            categories.append(term)

    return {
        "id": f"arxiv:{extract_arxiv_id(link)}",
        "title": title,
        "summary_raw": summary,
        "published": published,
        "url": link,
        "authors": authors,
        "institutions": [],
        "journal": "arXiv",
        "categories": categories,
        "source": "arXiv"
    }


def classify_method(text: str):
    t = text.lower()

    experimental_keys = [
        "experiment", "experimental", "laboratory", "specimen", "measured",
        "measurement", "field test", "field experiment", "testbed", "sensor"
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

    exp_hit = any(k in t for k in experimental_keys)
    num_hit = any(k in t for k in numerical_keys)
    ml_hit = any(k in t for k in ml_keys)
    theory_hit = any(k in t for k in theory_keys)
    review_hit = any(k in t for k in review_keys)

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


def infer_keywords(item, topic_name):
    text = (item["title"] + " " + item["summary_raw"] + " " + " ".join(item.get("categories", []))).lower()

    candidate_keywords = [
        "distributed acoustic sensing",
        "fiber optic sensing",
        "optical fiber sensing",
        "guided wave",
        "lamb wave",
        "ultrasonic",
        "acoustic emission",
        "structural health monitoring",
        "damage detection",
        "localization",
        "monitoring",
        "deep learning",
        "machine learning",
        "finite element",
        "numerical simulation"
    ]

    kws = [kw for kw in candidate_keywords if kw in text]
    if topic_name not in kws:
        kws.insert(0, topic_name)
    return kws[:6]


def build_summary_sentences(summary_raw: str, title: str, journal: str, method: str):
    summary_raw = clean_text(summary_raw)

    if summary_raw:
        parts = [p.strip() for p in re.split(r'(?<=[.!?])\s+', summary_raw) if p.strip()]
        return parts[:3] if parts else [summary_raw]

    fallback = [
        f"This paper is related to {title}.",
        f"It appears in {journal if journal else 'a selected source'}.",
        f"The likely research mode is {method}."
    ]
    return fallback


def build_conclusion_sentences(summary_raw: str, method: str):
    summary_raw = clean_text(summary_raw)

    if summary_raw:
        parts = [p.strip() for p in re.split(r'(?<=[.!?])\s+', summary_raw) if p.strip()]
        if len(parts) >= 2:
            return parts[-2:]
        return parts[:1]

    return [
        f"The paper appears to mainly rely on {method.lower()} methods.",
        "The detailed conclusions should be verified from the original paper."
    ]


def query_semantic_scholar_by_title(title: str):
    if not SEMANTIC_SCHOLAR_ENABLED:
        return None

    encoded = urllib.parse.quote(title)
    fields = ",".join([
        "title",
        "authors",
        "year",
        "publicationDate",
        "venue",
        "externalIds",
        "fieldsOfStudy"
    ])
    url = (
        "https://api.semanticscholar.org/graph/v1/paper/search?"
        f"query={encoded}&limit=1&fields={urllib.parse.quote(fields)}"
    )

    headers = {}
    if SEMANTIC_SCHOLAR_API_KEY:
        headers["x-api-key"] = SEMANTIC_SCHOLAR_API_KEY

    try:
        data = safe_json_request(url, headers=headers, timeout=30)
        time.sleep(0.6)
        papers = data.get("data", [])
        if not papers:
            return None
        return papers[0]
    except Exception as e:
        print(f"Semantic Scholar lookup failed for title '{title[:80]}': {e}")
        return None


def extract_institutions_from_semantic_scholar(paper_obj):
    institutions = []
    if not paper_obj:
        return institutions

    authors = paper_obj.get("authors", [])
    for author in authors:
        affs = author.get("affiliations", []) or []
        for aff in affs:
            aff_clean = clean_text(str(aff))
            if aff_clean and aff_clean not in institutions:
                institutions.append(aff_clean)

    return institutions[:6]


def enrich_with_semantic_scholar(raw_item):
    result = {
        "institutions": [],
        "fields_of_study": [],
        "venue": ""
    }

    paper_obj = query_semantic_scholar_by_title(raw_item["title"])
    if not paper_obj:
        return result

    result["institutions"] = extract_institutions_from_semantic_scholar(paper_obj)
    result["fields_of_study"] = [clean_text(str(x)) for x in (paper_obj.get("fieldsOfStudy", []) or []) if clean_text(str(x))][:6]
    result["venue"] = clean_text(str(paper_obj.get("venue", "")))

    return result


def item_matches_topic(item, topic_name, topic_keywords):
    text = " ".join([
        item.get("title", ""),
        item.get("summary_raw", ""),
        item.get("journal", ""),
        " ".join(item.get("categories", []))
    ]).lower()

    return any(kw.lower() in text for kw in topic_keywords)


def source_priority(item):
    # Journal papers first, arXiv second
    return 0 if item.get("source") == "Crossref" else 1


def build_final_item(raw_item, topic_name):
    combined_text = " ".join([
        raw_item.get("title", ""),
        raw_item.get("summary_raw", ""),
        raw_item.get("journal", ""),
        " ".join(raw_item.get("categories", []))
    ])

    method = classify_method(combined_text)

    institutions = raw_item.get("institutions", []) or []
    venue = raw_item.get("journal", "")
    fields_of_study = []

    if len(institutions) == 0:
        enriched = enrich_with_semantic_scholar(raw_item)
        institutions = enriched.get("institutions", []) or ["Not available from source"]
        if enriched.get("venue"):
            venue = enriched["venue"]
        fields_of_study = enriched.get("fields_of_study", [])
    else:
        if not venue:
            venue = raw_item.get("journal", "")

    return {
        "id": raw_item["id"],
        "topic": topic_name,
        "title": raw_item["title"],
        "authors": raw_item.get("authors", [])[:8],
        "institution": institutions[:6] if institutions else ["Not available from source"],
        "published": raw_item.get("published", ""),
        "keywords": infer_keywords(raw_item, topic_name),
        "method": method,
        "summary": build_summary_sentences(
            raw_item.get("summary_raw", ""),
            raw_item.get("title", ""),
            venue,
            method
        ),
        "conclusions": build_conclusion_sentences(
            raw_item.get("summary_raw", ""),
            method
        ),
        "source": raw_item.get("source", ""),
        "url": raw_item.get("url", ""),
        "categories": raw_item.get("categories", []),
        "venue": venue,
        "fields_of_study": fields_of_study
    }


def main():
    ensure_dir(DAILY_DIR)

    topics_map = load_json(TOPICS_FILE, {})
    journals = load_json(JOURNALS_FILE, [])
    seen = load_json(SEEN_FILE, {"featured_ids": [], "featured_titles": []})

    selected_journals = {normalized_journal_name(j) for j in journals}
    featured_ids = set(seen.get("featured_ids", []))
    featured_titles = set(seen.get("featured_titles", []))

    results_by_topic = {
        topic: {"fresh": [], "backlog": [], "highlights": []}
        for topic in topics_map.keys()
    }

    newly_featured_ids = []
    newly_featured_titles = []

    for topic_name, keywords in topics_map.items():
        candidate_items = []

        # ---- Crossref journal-focused retrieval ----
        for kw in keywords:
            items = query_crossref(kw, rows=CROSSREF_ROWS_PER_KEYWORD)

            for raw in items:
                parsed = parse_crossref_item(raw)

                if not parsed["title"] or not parsed["url"]:
                    continue

                if not parsed["journal"]:
                    continue

                if normalized_journal_name(parsed["journal"]) not in selected_journals:
                    continue

                if not parsed["published"]:
                    continue

                if days_since(parsed["published"]) > LOOKBACK_DAYS:
                    continue

                if not item_matches_topic(parsed, topic_name, keywords):
                    continue

                norm_title = normalize_title(parsed["title"])
                if parsed["id"] in featured_ids or norm_title in featured_titles:
                    continue

                candidate_items.append(parsed)

        # ---- arXiv supplement ----
        for kw in keywords:
            xml_bytes = query_arxiv(kw, max_results=ARXIV_ROWS_PER_KEYWORD)
            entries = parse_entries(xml_bytes)

            for entry in entries:
                parsed = parse_arxiv_entry(entry)

                if not parsed["title"] or not parsed["url"]:
                    continue

                if not parsed["published"]:
                    continue

                if days_since(parsed["published"]) > LOOKBACK_DAYS:
                    continue

                if not item_matches_topic(parsed, topic_name, keywords):
                    continue

                norm_title = normalize_title(parsed["title"])
                if parsed["id"] in featured_ids or norm_title in featured_titles:
                    continue

                candidate_items.append(parsed)

        # ---- local dedup ----
        deduped = []
        local_ids = set()
        local_titles = set()

        for item in sorted(candidate_items, key=source_priority):
            norm_title = normalize_title(item["title"])
            if item["id"] in local_ids or norm_title in local_titles:
                continue
            local_ids.add(item["id"])
            local_titles.add(norm_title)
            deduped.append(item)

        # ---- bucket fill ----
        bucketed = {"fresh": [], "backlog": [], "highlights": []}

        for raw_item in deduped:
            bucket = classify_bucket(raw_item["published"])
            final_item = build_final_item(raw_item, topic_name)

            if len(bucketed[bucket]) < TARGET_COUNTS[bucket]:
                bucketed[bucket].append(final_item)

        results_by_topic[topic_name] = bucketed

        for bucket_name in ["fresh", "backlog", "highlights"]:
            for item in bucketed[bucket_name]:
                norm_title = normalize_title(item["title"])
                newly_featured_ids.append(item["id"])
                newly_featured_titles.append(norm_title)

    # fallback if absolutely empty
    all_empty = True
    for topic_payload in results_by_topic.values():
        if any(len(topic_payload[b]) > 0 for b in ["fresh", "backlog", "highlights"]):
            all_empty = False
            break

    if all_empty:
        print("No new topic items found. Falling back to the latest previous non-empty daily file.")
        existing_files = sorted(DAILY_DIR.glob("*.json"), reverse=True)
        for f in existing_files:
            if f.stem == today_str():
                continue
            prev = load_json(f, {})
            prev_topics = prev.get("topics", {})
            has_any = False
            for topic_payload in prev_topics.values():
                if any(len(topic_payload.get(b, [])) > 0 for b in ["fresh", "backlog", "highlights"]):
                    has_any = True
                    break
            if has_any:
                results_by_topic = prev_topics
                break
    else:
        seen["featured_ids"] = sorted(list(set(featured_ids.union(newly_featured_ids))))
        seen["featured_titles"] = sorted(list(set(featured_titles.union(newly_featured_titles))))
        save_json(SEEN_FILE, seen)

    out = {
        "date": today_str(),
        "topics": results_by_topic
    }

    out_file = DAILY_DIR / f"{today_str()}.json"
    save_json(out_file, out)
    print(f"Saved research data to {out_file}")


if __name__ == "__main__":
    main()
