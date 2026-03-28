import json
import re
from pathlib import Path
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[1]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def normalize_title(title: str) -> str:
    s = title.lower().strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def clean_text(text: str) -> str:
    text = text.replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def extract_arxiv_id(url: str) -> str:
    if not url:
        return ""
    return url.rstrip("/").split("/")[-1]
