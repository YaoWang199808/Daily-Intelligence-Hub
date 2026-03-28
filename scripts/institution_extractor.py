import urllib.request
import re

def fetch_html(url):
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.read().decode("utf-8", errors="ignore")
    except:
        return ""


def extract_elsevier(html):
    """
    Elsevier (ScienceDirect) extraction
    """
    matches = re.findall(r'"affiliation":\{"name":"(.*?)"', html)
    return list(set(matches))


def extract_springer(html):
    """
    Springer extraction
    """
    matches = re.findall(r'<span class="affiliation__name">(.*?)</span>', html)
    return list(set([clean_text(m) for m in matches]))


def extract_general(html):
    """
    fallback: try to grab university-like strings
    """
    candidates = re.findall(r'([A-Z][A-Za-z\s,]+University[^<,]*)', html)
    return list(set(candidates))


def clean_text(text):
    text = re.sub(r'<.*?>', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def extract_institutions_from_url(url):
    html = fetch_html(url)
    if not html:
        return []

    # Elsevier
    inst = extract_elsevier(html)
    if inst:
        return inst[:6]

    # Springer
    inst = extract_springer(html)
    if inst:
        return inst[:6]

    # fallback
    inst = extract_general(html)
    return inst[:6]
