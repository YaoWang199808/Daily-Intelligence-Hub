from pathlib import Path
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
import urllib.parse
import urllib.request
import time
import os
import re

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
SEEN_FILE = DATA_DIR / "seen.json"
DAILY_DIR = DATA_DIR / "daily"

ARXIV_NS = {"a": "http://www.w3.org/2005/Atom"}
MAX_PER_TOPIC = 20
QUERY_RESULTS = 100
DAYS_BACK = 14

# Optional Semantic Scholar API key
SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY", "").strip()
SEMANTIC_SCHOLAR_ENABLED = True


def query_arxiv(keyword: str, max_results: int = QUERY_RESULTS):
    query = urllib.parse.quote(f'all:"{keyword}"')
    url = (
        "http://export.arxiv.org/api/query?"
        f"search_query={query}&start=0&max_results={max_results}"
        "&sortBy=submittedDate&sortOrder=descending"
    )
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read()


def parse_entries(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)
    return root.findall("a:entry", ARXIV_NS)


def text_of(elem, path: str) -> str:
    found = elem.find(path, ARXIV_NS)
    return found.text.strip() if found is not None and found.text else ""


def parse_entry(entry):
    title = clean_text(text_of(entry, "a:title"))
    summary = clean_text(text_of(entry, "a:summary"))
    published = text_of(entry, "a:published")
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
        "title": title,
        "summary_raw": summary,
        "published": published,
        "url": link,
        "authors": authors,
        "categories": categories,
        "id": f"arxiv:{extract_arxiv_id(link)}",
    }


def is_recent(published_str: str, days_back: int = DAYS_BACK) -> bool:
    if not published_str:
        return False
    try:
        dt = datetime.fromisoformat(published_str.replace("Z", "+00:00"))
    except ValueError:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    return dt >= cutoff


def topic_score(topic_keywords, text: str) -> int:
    text_lower = text.lower()
    score = 0
    for kw in topic_keywords:
        if kw.lower() in text_lower:
            score += 1
    return score


def classify_topic(item, topics_map):
    combined = " ".join(
        [
            item["title"],
            item["summary_raw"],
            " ".join(item["categories"]),
        ]
    )
    best_topic = None
    best_score = -1
    for topic, kws in topics_map.items():
        score = topic_score(kws, combined)
        if score > best_score:
            best_score = score
            best_topic = topic
    return best_topic if best_score > 0 else None


def classify_method(text: str):
    """
    Return one of:
    - Experimental
    - Numerical Simulation
    - Machine Learning
    - Hybrid
    - Analytical / Theoretical
    - Review / Survey
    - Other
    """
    t = text.lower()

    experimental_keys = [
        "experiment", "experimental", "tested", "testbed", "laboratory",
        "lab-scale", "specimen", "measurement", "sensor measurement",
        "field test", "field experiment", "validation experiment"
    ]
    numerical_keys = [
        "simulation", "numerical", "finite element", "finite-element",
        "fem", "modeling", "modelling", "comsol", "abaqus"
    ]
    ml_keys = [
        "machine learning", "deep learning", "neural network", "cnn", "rnn",
        "transformer", "classification model", "random forest", "svm",
        "support vector machine", "xgboost", "artificial intelligence"
    ]
    theory_keys = [
        "analytical", "theoretical", "closed-form", "theory", "derived",
        "derivation", "formula", "mathematical model"
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

    positive_count = sum([exp_hit, num_hit, ml_hit, theory_hit])

    if positive_count >= 2:
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
    kws = []
    title_summary = (item["title"] + " " + item["summary_raw"]).lower()

    candidate_keywords = [
        "distributed acoustic sensing",
        "fiber optic sensing",
        "crack detection",
        "crack monitoring",
        "guided wave",
        "lamb wave",
        "acoustic emission",
        "structural health monitoring",
        "rock dynamics",
        "deep learning",
        "machine learning",
        "finite element",
        "numerical simulation",
        "damage detection",
        "ultrasonic",
        "fracture",
        "localization"
    ]

    for kw in candidate_keywords:
        if kw in title_summary:
            kws.append(kw)

    if topic_name not in kws:
        kws.insert(0, topic_name)

    return kws[:6]


def build_summary_sentences(summary_raw: str):
    summary_raw = clean_text(summary_raw)
    if not summary_raw:
        return ["No summary available."]
    parts = [p.strip() for p in re.split(r'(?<=[.!?])\s+', summary_raw) if p.strip()]
    return parts[:3] if parts else [summary_raw]


def build_conclusion_sentences(summary_raw: str):
    summary_raw = clean_text(summary_raw)
    if not summary_raw:
        return ["No conclusion summary available."]
    parts = [p.strip() for p in re.split(r'(?<=[.!?])\s+', summary_raw) if p.strip()]
    if len(parts) >= 2:
        return parts[-2:]
    return parts[:1]


def safe_json_request(url: str, headers=None, timeout=30):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        import json
        return json.loads(response.read().decode("utf-8"))


def query_semantic_scholar_by_title(title: str):
    """
    Query Semantic Scholar paper search endpoint by title.
    Works without API key for low-volume usage.
    """
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
        time.sleep(0.7)  # gentle rate limiting
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
    """
    Fallback-safe enrichment.
    Returns dict with optional:
    - institutions
    - fields_of_study
    - venue
    """
    result = {
        "institutions": [],
        "fields_of_study": [],
        "venue": ""
    }

    paper_obj = query_semantic_scholar_by_title(raw_item["title"])
    if not paper_obj:
        return result

    result["institutions"] = extract_institutions_from_semantic_scholar(paper_obj)

    fos = paper_obj.get("fieldsOfStudy", []) or []
    result["fields_of_study"] = [clean_text(str(x)) for x in fos if clean_text(str(x))][:6]

    venue = clean_text(str(paper_obj.get("venue", "")))
    result["venue"] = venue

    return result


def main():
    ensure_dir(DAILY_DIR)

    topics_map = load_json(TOPICS_FILE, {})
    seen = load_json(SEEN_FILE, {"seen_ids": [], "seen_titles": []})

    seen_ids = set(seen.get("seen_ids", []))
    seen_titles = set(seen.get("seen_titles", []))

    results_by_topic = {topic: [] for topic in topics_map.keys()}
    added_ids = []
    added_titles = []

    for topic_name, keywords in topics_map.items():
        topic_candidates = []

        for kw in keywords:
            try:
                xml_bytes = query_arxiv(kw)
                entries = parse_entries(xml_bytes)
            except Exception as e:
                print(f"Failed query for {kw}: {e}")
                continue

            for entry in entries:
                raw_item = parse_entry(entry)

                if not is_recent(raw_item["published"]):
                    continue

                norm_title = normalize_title(raw_item["title"])
                if raw_item["id"] in seen_ids or norm_title in seen_titles:
                    continue

                combined_text = raw_item["title"] + " " + raw_item["summary_raw"]
                method = classify_method(combined_text)

                enriched = enrich_with_semantic_scholar(raw_item)
                institutions = enriched.get("institutions", []) or ["Not available from source"]

                item = {
                    "id": raw_item["id"],
                    "topic": topic_name,
                    "title": raw_item["title"],
                    "authors": raw_item["authors"][:8],
                    "institution": institutions,
                    "published": raw_item["published"][:10],
                    "keywords": infer_keywords(raw_item, topic_name),
                    "method": method,
                    "summary": build_summary_sentences(raw_item["summary_raw"]),
                    "conclusions": build_conclusion_sentences(raw_item["summary_raw"]),
                    "source": "arXiv",
                    "url": raw_item["url"],
                    "categories": raw_item["categories"],
                    "venue": enriched.get("venue", ""),
                    "fields_of_study": enriched.get("fields_of_study", []),
                }

                topic_candidates.append(item)

        unique_topic_items = []
        local_ids = set()
        local_titles = set()

        for item in topic_candidates:
            norm_title = normalize_title(item["title"])
            if item["id"] in local_ids or norm_title in local_titles:
                continue
            local_ids.add(item["id"])
            local_titles.add(norm_title)
            unique_topic_items.append(item)

        unique_topic_items = sorted(
            unique_topic_items,
            key=lambda x: x["published"],
            reverse=True
        )[:MAX_PER_TOPIC]

        results_by_topic[topic_name] = unique_topic_items

        for item in unique_topic_items:
            norm_title = normalize_title(item["title"])
            added_ids.append(item["id"])
            added_titles.append(norm_title)

    seen["seen_ids"] = sorted(list(set(seen_ids.union(added_ids))))
    seen["seen_titles"] = sorted(list(set(seen_titles.union(added_titles))))
    save_json(SEEN_FILE, seen)

    # Fallback: if today is empty, reuse last available data
    all_empty = all(len(v) == 0 for v in results_by_topic.values())

    if all_empty:
        print("No new items found today. Falling back to the most recent previous non-empty data.")
        existing_files = sorted(DAILY_DIR.glob("*.json"), reverse=True)

        for f in existing_files:
            if f.stem == today_str():
                continue

            prev_data = load_json(f, {})
            prev_topics = prev_data.get("topics", {})

            if any(len(items) > 0 for items in prev_topics.values()):
                results_by_topic = prev_topics
                break

    out = {
        "date": today_str(),
        "topics": results_by_topic
    }

    out_file = DAILY_DIR / f"{today_str()}.json"
    save_json(out_file, out)
    print(f"Saved research data to {out_file}")


if __name__ == "__main__":
    main()
