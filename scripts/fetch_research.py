from pathlib import Path
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
import urllib.parse
import urllib.request
import json

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
QUERY_RESULTS = 30
DAYS_BACK = 7


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


def classify_method(text):
    text = text.lower()

    if "deep learning" in text or "neural network" in text:
        return "Machine Learning"
    if "finite element" in text or "simulation" in text:
        return "Numerical Simulation"
    if "experiment" in text or "experimental" in text:
        return "Experimental"
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
    parts = [p.strip() for p in summary_raw.split(". ") if p.strip()]
    parts = [p if p.endswith(".") else p + "." for p in parts]
    return parts[:3] if parts else [summary_raw]


def build_conclusion_sentences(summary_raw: str):
    summary_raw = clean_text(summary_raw)
    if not summary_raw:
        return ["No conclusion summary available."]

    parts = [p.strip() for p in summary_raw.split(". ") if p.strip()]
    parts = [p if p.endswith(".") else p + "." for p in parts]

    if len(parts) >= 2:
        return parts[-2:]
    return parts[:1]


def infer_institutions(_item):
    return ["Not available from source"]


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

                assigned_topic = classify_topic(raw_item, topics_map)
                if assigned_topic != topic_name:
                    continue

                item = {
                    "id": raw_item["id"],
                    "topic": topic_name,
                    "title": raw_item["title"],
                    "authors": raw_item["authors"][:8],
                    "institution": infer_institutions(raw_item),
                    "published": raw_item["published"][:10],
                    "keywords": infer_keywords(raw_item, topic_name),
                    "method": infer_method(raw_item),
                    "summary": build_summary_sentences(raw_item["summary_raw"]),
                    "conclusions": build_conclusion_sentences(raw_item["summary_raw"]),
                    "source": "arXiv",
                    "url": raw_item["url"],
                    "categories": raw_item["categories"],
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

    out = {
        "date": today_str(),
        "topics": results_by_topic
    }

    out_file = DAILY_DIR / f"{today_str()}.json"
    save_json(out_file, out)
    print(f"Saved research data to {out_file}")


if __name__ == "__main__":
    main()
