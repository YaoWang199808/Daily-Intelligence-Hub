from pathlib import Path
from datetime import datetime, timezone, timedelta
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

SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY", "").strip()

CURRENT_YEAR = datetime.now(timezone.utc).year
LOOKBACK_YEARS = 8

NEW_PER_JOURNAL = 5
CITED_PER_JOURNAL = 5
MAX_PER_JOURNAL = 10

CROSSREF_ROWS_PER_JOURNAL = 300

# 期刊别名，做“严格验证”
JOURNAL_ALIASES = {
    "Automation in Construction": [
        "automation in construction"
    ],
    "Mechanical Systems and Signal Processing": [
        "mechanical systems and signal processing"
    ],
    "Measurement": [
        "measurement"
    ],
    "Engineering Structures": [
        "engineering structures"
    ],
    "Ultrasonics": [
        "ultrasonics"
    ],
    "Tunnelling and Underground Space Technology": [
        "tunnelling and underground space technology",
        "tunneling and underground space technology"
    ],
    "Rock Mechanics and Rock Engineering": [
        "rock mechanics and rock engineering"
    ],
}


def normalized_text(text: str) -> str:
    return clean_text(text).lower()


def html_strip(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return html.unescape(text).strip()


def safe_json_request(url: str, headers=None, timeout=30):
    req = urllib.request.Request(
        url,
        headers=headers or {"User-Agent": "Daily-Intelligence-Hub/1.0"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_html(url: str, timeout=25):
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as response:
            final_url = response.geturl()
            content = response.read().decode("utf-8", errors="ignore")
            return final_url, content
    except Exception:
        return url, ""


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


def journal_match(found_journal: str, target_journal: str) -> bool:
    found = normalized_text(found_journal)
    aliases = JOURNAL_ALIASES.get(target_journal, [normalized_text(target_journal)])
    return any(found == a for a in aliases)


def query_crossref_for_journal(journal_name: str, rows: int = CROSSREF_ROWS_PER_JOURNAL):
    from_date = (
        datetime.now(timezone.utc) - timedelta(days=365 * LOOKBACK_YEARS)
    ).strftime("%Y-%m-%d")

    encoded_journal = urllib.parse.quote(journal_name)

    url = (
        "https://api.crossref.org/works?"
        f"query={encoded_journal}"
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
        "institutions": institutions[:8],
        "journal": journal
    }


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

    return institutions[:8]


def enrich_with_semantic_scholar(item):
    s2 = None

    if item.get("doi"):
        s2 = query_semantic_scholar_by_doi(item["doi"])

    if not s2:
        s2 = query_semantic_scholar_by_title(item.get("title", ""))

    time.sleep(0.35)

    if not s2:
        return {
            "citation_count": 0,
            "institutions": []
        }

    return {
        "citation_count": int(s2.get("citationCount", 0) or 0),
        "institutions": extract_institutions_from_s2(s2)
    }


def extract_meta_tags(html_text: str):
    """
    More robust meta parser:
    supports name/content or property/content regardless of order.
    """
    meta = {}

    patterns = [
        re.compile(
            r'<meta\s+[^>]*(?:name|property)=["\']([^"\']+)["\'][^>]*content=["\']([^"\']*)["\'][^>]*>',
            re.IGNORECASE
        ),
        re.compile(
            r'<meta\s+[^>]*content=["\']([^"\']*)["\'][^>]*(?:name|property)=["\']([^"\']+)["\'][^>]*>',
            re.IGNORECASE
        ),
    ]

    for pattern in patterns:
        for m in pattern.findall(html_text):
            if len(m) != 2:
                continue
            if pattern is patterns[0]:
                key, value = m
            else:
                value, key = m
            key = key.strip().lower()
            value = html.unescape(value.strip())
            if key not in meta:
                meta[key] = []
            if value:
                meta[key].append(value)

    return meta


def extract_jsonld_blocks(html_text: str):
    blocks = re.findall(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html_text,
        flags=re.IGNORECASE | re.DOTALL
    )
    objs = []
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        try:
            objs.append(json.loads(block))
        except Exception:
            continue
    return objs


def flatten_jsonld_objects(obj):
    if isinstance(obj, list):
        for x in obj:
            yield from flatten_jsonld_objects(x)
    elif isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from flatten_jsonld_objects(v)


def pick_first_nonempty(candidates):
    for x in candidates:
        if x and clean_text(str(x)):
            return clean_text(str(x))
    return ""


def extract_authors_from_page(meta, jsonlds):
    authors = []

    for v in meta.get("citation_author", []):
        val = clean_text(v)
        if val and val not in authors:
            authors.append(val)

    if authors:
        return authors[:10]

    for obj in jsonlds:
        for node in flatten_jsonld_objects(obj):
            if node.get("@type") in ["ScholarlyArticle", "Article", "NewsArticle"]:
                a = node.get("author")
                if isinstance(a, list):
                    for item in a:
                        if isinstance(item, dict):
                            name = clean_text(item.get("name", ""))
                            if name and name not in authors:
                                authors.append(name)
                elif isinstance(a, dict):
                    name = clean_text(a.get("name", ""))
                    if name and name not in authors:
                        authors.append(name)

    return authors[:10]


def extract_institutions_from_page(meta, jsonlds):
    institutions = []

    for v in meta.get("citation_author_institution", []):
        val = clean_text(v)
        if val and val not in institutions:
            institutions.append(val)

    if institutions:
        return institutions[:8]

    for obj in jsonlds:
        for node in flatten_jsonld_objects(obj):
            if node.get("@type") in ["ScholarlyArticle", "Article", "NewsArticle"]:
                authors = node.get("author")
                if isinstance(authors, list):
                    for a in authors:
                        if isinstance(a, dict):
                            aff = a.get("affiliation")
                            if isinstance(aff, list):
                                for item in aff:
                                    if isinstance(item, dict):
                                        name = clean_text(item.get("name", ""))
                                        if name and name not in institutions:
                                            institutions.append(name)
                                    else:
                                        name = clean_text(str(item))
                                        if name and name not in institutions:
                                            institutions.append(name)
                            elif isinstance(aff, dict):
                                name = clean_text(aff.get("name", ""))
                                if name and name not in institutions:
                                    institutions.append(name)
                            elif aff:
                                name = clean_text(str(aff))
                                if name and name not in institutions:
                                    institutions.append(name)

    return institutions[:8]


def extract_abstract_from_page(meta, jsonlds):
    candidates = []
    for key in [
        "citation_abstract",
        "dc.description",
        "description",
        "og:description",
        "twitter:description"
    ]:
        candidates.extend(meta.get(key, []))

    abstract_text = pick_first_nonempty(candidates)
    if abstract_text:
        return html_strip(abstract_text)

    for obj in jsonlds:
        for node in flatten_jsonld_objects(obj):
            if node.get("@type") in ["ScholarlyArticle", "Article", "NewsArticle"]:
                desc = pick_first_nonempty([
                    node.get("description", ""),
                    node.get("abstract", "")
                ])
                if desc:
                    return html_strip(desc)

    return ""


def extract_journal_from_page(meta, jsonlds):
    candidates = []
    for key in [
        "citation_journal_title",
        "prism.publicationname",
        "dc.source"
    ]:
        candidates.extend(meta.get(key, []))

    journal_text = pick_first_nonempty(candidates)
    if journal_text:
        return journal_text

    for obj in jsonlds:
        for node in flatten_jsonld_objects(obj):
            part_of = node.get("isPartOf")
            if isinstance(part_of, dict):
                name = clean_text(part_of.get("name", ""))
                if name:
                    return name

    return ""


def extract_published_from_page(meta, jsonlds):
    candidates = []
    for key in [
        "citation_publication_date",
        "prism.publicationdate",
        "dc.date",
        "article:published_time"
    ]:
        candidates.extend(meta.get(key, []))

    date_text = pick_first_nonempty(candidates)
    if date_text:
        return date_text[:10]

    for obj in jsonlds:
        for node in flatten_jsonld_objects(obj):
            date_published = clean_text(node.get("datePublished", ""))
            if date_published:
                return date_published[:10]

    return ""


def extract_page_metadata(url: str):
    final_url, html_text = fetch_html(url)
    if not html_text:
        return {}

    meta = extract_meta_tags(html_text)
    jsonlds = extract_jsonld_blocks(html_text)

    return {
        "final_url": final_url,
        "authors": extract_authors_from_page(meta, jsonlds),
        "institutions": extract_institutions_from_page(meta, jsonlds),
        "abstract": extract_abstract_from_page(meta, jsonlds),
        "journal": extract_journal_from_page(meta, jsonlds),
        "published": extract_published_from_page(meta, jsonlds),
    }


def build_final_item(raw_item):
    # Page extraction first
    page_meta = extract_page_metadata(raw_item.get("url", ""))

    authors = raw_item.get("authors", []) or []
    institutions = raw_item.get("institutions", []) or []
    journal = raw_item.get("journal", "")
    published = raw_item.get("published", "")
    abstract_text = clean_text(raw_item.get("abstract_raw", ""))

    if page_meta.get("authors"):
        authors = page_meta["authors"]
    if page_meta.get("institutions"):
        institutions = page_meta["institutions"]
    if page_meta.get("journal"):
        journal = page_meta["journal"]
    if page_meta.get("published"):
        published = page_meta["published"]
    if page_meta.get("abstract"):
        abstract_text = page_meta["abstract"]

    # Strict validation: page journal must match target later in main()
    # Here only enrich citations / fallback institution
    enriched = enrich_with_semantic_scholar(raw_item)
    citation_count = enriched.get("citation_count", 0)

    if not institutions:
        institutions = enriched.get("institutions", []) or []

    if not abstract_text:
        abstract_text = "Abstract not available."

    combined_text = " ".join([
        raw_item.get("title", ""),
        abstract_text,
        journal
    ]).lower()

    method = classify_method(combined_text)

    return {
        "id": raw_item["id"],
        "title": raw_item["title"],
        "authors": authors[:10],
        "institution": institutions[:8],
        "published": published,
        "abstract": abstract_text,
        "url": raw_item.get("url", ""),
        "journal": journal,
        "citation_count": citation_count,
        "method": method
    }


def select_items_for_journal(enriched_items):
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

    selected_ids.update(x["id"] for x in cited_items)

    filler_candidates = [
        x for x in enriched_items if x["id"] not in selected_ids
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

    for target_journal in journals:
        raw_items = query_crossref_for_journal(target_journal, rows=CROSSREF_ROWS_PER_JOURNAL)

        parsed_items = []
        for raw in raw_items:
            parsed = parse_crossref_item(raw)

            if not parsed["title"] or not parsed["url"] or not parsed["published"]:
                continue

            # first pass journal validation on Crossref metadata
            if not journal_match(parsed["journal"], target_journal):
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

        # build + second pass strict journal validation using page-extracted journal if available
        enriched_items = []
        for item in deduped:
            built = build_final_item(item)
            # If page extracted / final journal exists, require it to still match target journal
            final_journal = built.get("journal", "") or item.get("journal", "")
            if not journal_match(final_journal, target_journal):
                continue
            enriched_items.append(built)

        final_items = select_items_for_journal(enriched_items)
        results_by_journal[target_journal] = final_items

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
