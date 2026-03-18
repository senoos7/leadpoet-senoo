"""
senoo_v0 — qualification model
Inherits the multi-probe pipeline (LinkedIn, jobs, news, website, DDG, wiki)
with hybrid rerank, LLM verification, and adaptive learning.
"""
import os
import re
import json
import math
import time
import random
import logging
import traceback
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional, List, Tuple, Set
from urllib.parse import quote, urlparse
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from rapidfuzz import fuzz as _fuzz
except Exception:
    from difflib import SequenceMatcher as _SM

    class _FuzzFallback:
        @staticmethod
        def ratio(a: str, b: str) -> float:
            if not a and not b:
                return 100.0
            return _SM(None, a, b).ratio() * 100.0

        @staticmethod
        def partial_ratio(a: str, b: str) -> float:
            if len(a) > len(b):
                a, b = b, a
            return _FuzzFallback.ratio(a, b)

    _fuzz = _FuzzFallback()

logger = logging.getLogger(__name__)

_DEBUG_QUALIFY = os.environ.get("DEBUG_SENOO", os.environ.get("DEBUG_QUALIFY", "")).lower() in ("1", "true", "yes")

def _debug_log(msg: str, *args: Any, **kwargs: Any) -> None:
    if _DEBUG_QUALIFY:
        logger.debug(msg, *args, **kwargs)

SENIORITY_ENUM = {"C-Suite", "VP", "Director", "Manager",
                  "Individual Contributor"}

INTENT_SOURCE_ENUM = {"linkedin", "job_board", "github", "news",
                      "company_website", "social_media", "review_site",
                      "wikipedia", "other"}

SOURCE_MULTIPLIERS: Dict[str, float] = {
    "linkedin": 1.0, "job_board": 1.0, "github": 1.0,
    "news": 0.92, "company_website": 0.85, "social_media": 0.78,
    "review_site": 0.72, "wikipedia": 0.55, "other": 0.3,
}

ROLE_TYPE_SENIORITY = {
    "C-Level Executive": "C-Suite", "Founder/Owner": "C-Suite",
    "VP/SVP": "VP",
    "Director": "Director",
    "Manager": "Manager", "Team Lead": "Manager",
    "Senior Professional": "Individual Contributor",
    "Engineer/Technical": "Individual Contributor",
    "Sales": "Individual Contributor",
    "Marketing": "Individual Contributor",
    "Operations": "Individual Contributor",
    "Finance/Accounting": "Individual Contributor",
    "HR/People": "Individual Contributor",
    "Legal": "Individual Contributor",
    "Support/Service": "Individual Contributor",
    "Product": "Individual Contributor",
    "Design": "Individual Contributor",
    "Data/Analytics": "Individual Contributor",
    "Compliance": "Individual Contributor",
    "Risk Management": "Individual Contributor",
    "Other": "Individual Contributor",
}

ROLE_SYNONYM_GROUPS: List[Set[str]] = [
    {"vp sales", "head of sales", "chief revenue officer", "cro", "vp of sales",
     "head of revenue", "vp revenue", "sales director"},
    {"vp marketing", "head of marketing", "cmo", "vp of marketing",
     "chief marketing officer", "marketing director"},
    {"vp engineering", "head of engineering", "cto", "vp of engineering",
     "chief technology officer", "engineering director", "head of devops"},
    {"vp product", "head of product", "cpo", "vp of product", "product director"},
    {"vp operations", "head of operations", "coo", "vp of operations"},
    {"vp customer success", "head of customer success", "vp of customer success"},
    {"vp partnerships", "head of partnerships", "vp of partnerships"},
    {"vp finance", "cfo", "head of finance", "chief financial officer"},
    {"vp hr", "head of people", "chief people officer", "chtro", "vp of hr"},
    {"vp compliance", "chief compliance officer", "head of compliance"},
    {"vp it", "head of it", "cio", "chief information officer"},
]

DECISION_MAKER_ANTI_PATTERNS = frozenset({
    "coordinator", "assistant", "intern", "junior", "jr.", "associate",
    "analyst", "specialist", "representative", "sales ops analyst",
    "support ", "admin", "entry level", "entry-level",
})

DECISION_MAKER_GENERIC_ROLES = frozenset({
    "team member", "staff", "employee", "worker", "member",
})

SENIORITY_RANK = {"C-Suite": 5, "VP": 4, "Director": 3, "Manager": 2, "Individual Contributor": 1}

def _normalize_title(role: str) -> str:
    r = _sl(role)
    for old, new in [
        ("head of", "head_of"),
        ("chief ", "chief_"),
        ("vp ", "vp_"),
        ("sr ", "sr_"),
        ("senior ", "sr_"),
        (" vp ", " vp_"),
    ]:
        r = r.replace(old, new)
    return r

def _role_synonym_match(lead_role: str, target_roles: List[str]) -> float:
    if not target_roles:
        return 0.5
    lead_norm = _normalize_title(lead_role)
    lead_tokens = set(lead_norm.split())
    best = 0.0
    for tr in target_roles:
        tr_norm = _normalize_title(tr)
        ratio = _fuzz.partial_ratio(lead_role, tr) / 100.0
        if ratio > best:
            best = ratio
        for group in ROLE_SYNONYM_GROUPS:
            lead_in = any(g in lead_norm for g in group)
            tr_in = any(g in _sl(tr) for g in group)
            if lead_in and tr_in:
                best = max(best, 0.95)
    return min(1.0, best)

JOB_BOARD_DOMAINS = frozenset([
    "indeed.com", "glassdoor.com", "dice.com", "ziprecruiter.com",
    "greenhouse.io", "lever.co", "workable.com", "ashbyhq.com",
    "smartrecruiters.com", "myworkdayjobs.com",
])

BLOCKED_DOMAINS = frozenset([
    "facebook.com", "instagram.com", "tiktok.com", "pinterest.com",
    "youtube.com", "reddit.com", "amazon.com",
])

SPECIFIC_INTENT_KW = frozenset([
    "hiring", "recruit", "job", "position", "opening",
    "launch", "released", "announced", "introduced",
    "raised", "funding", "series", "investment",
    "partnership", "partnered", "collaboration",
    "acquired", "acquisition", "merger",
    "expansion", "opened", "new office", "new location",
    "migrating", "adopting", "implementing",
    "awarded", "contract", "signed", "IPO",
])

EVIDENCE_ANCHORS = [
    "hiring", "open role", "job posting", "posted",
    "funding", "raised", "series", "investment",
    "launched", "announced", "partnership", "expansion",
    "acquired", "merger", "new office", "awarded", "signed contract",
]

_HTTP_TIMEOUT = 5.0
_SCRAPE_TIMEOUT = 5.0
_LLM_TIMEOUT = 8.0
_PIPELINE_BUDGET = 24.5
_CONTENT_MAX = 4800

_ICP_INTENT_CATEGORIES: Dict[str, List[str]] = {
    "funding": [
        "raised funding", "closed round", "closed a round", "just raised",
        "recently raised", "series a", "series b", "series c", "series d",
        "seed round", "investment", "funding round", "capital raise",
        "financing", "raised", "funding",
    ],
    "hiring": [
        "hiring", "recruit", "growing team", "expanding team", "talent",
        "headcount", "new hires", "ramping up", "building out",
    ],
    "transformation": [
        "digital transformation", "modernization", "cloud migration",
        "technology adoption", "tech adoption", "digitalization",
        "undergoing digital", "digital overhaul",
    ],
    "expansion": [
        "expanding to new market", "new location", "new office",
        "geographic expansion", "entering", "opening new",
        "global expansion", "expanding", "new markets",
    ],
    "evaluation": [
        "evaluating new", "evaluating", "new solutions", "replacing",
        "procurement", "vendor selection", "rfp", "new options",
        "implementing", "migrating", "new platform", "new tool",
        "cloud security options", "shortlisting", "pilot program",
    ],
    "conference": [
        "conference", "spoke at", "speaker", "keynote", "event",
        "summit", "webinar", "presentation", "panel", "spoke at conference",
        "roundtable", "trade show", "expo",
    ],
}

_INTENT_ACTION_KEYWORDS: Dict[str, List[str]] = {
    "funding": ["raised", "closed", "secured", "funding", "series", "round",
                 "invested", "capital", "financing", "backed", "led by"],
    "hiring": ["hired", "hiring", "recruiting", "job", "position", "opening",
                "headcount", "talent", "onboarding", "roles"],
    "transformation": ["launched", "migrating", "adopting", "implementing",
                        "deploying", "transforming", "modernizing", "overhauling"],
    "expansion": ["expanded", "opening", "entering", "launched", "new market",
                   "new office", "new region", "new country", "global"],
    "evaluation": ["evaluating", "replacing", "implementing", "migrating",
                    "selected", "deployed", "piloting", "trialing", "procuring",
                    "shortlisting", "awarded", "signed"],
    "conference": ["spoke", "presented", "keynote", "panelist", "attended",
                    "hosted", "moderated", "featured"],
}

_WEAK_PAGE_URL_PATTERNS = [
    r'/about(?:-us)?/?$', r'/team\b', r'/leadership\b', r'/board\b',
    r'/our-story', r'/our-values', r'/our-mission',
    r'/podcast', r'/profile', r'/bio\b', r'/author/',
    r'/whitepaper', r'/ebook\b', r'/webinar(?:-archive)?',
    r'/privacy', r'/terms', r'/cookie', r'/legal',
    r'/sitemap', r'/contact(?:-us)?/?$', r'/faq\b',
]

_WEAK_CONTENT_SIGNALS = [
    "meet our team", "our leadership", "board of directors",
    "about us", "our mission", "our vision", "our values",
    "listen to the podcast", "subscribe to our podcast",
    "download the whitepaper", "download the ebook",
    "privacy policy", "terms of service", "cookie policy",
    "all rights reserved", "unsubscribe", "manage preferences",
]

_VAGUE_PHRASES = frozenset([
    "indicating potential", "suggesting a focus", "may imply",
    "possibly", "could suggest", "appears to", "seems to",
    "may indicate", "might suggest", "potentially",
    "indicating a commitment", "indicating growth",
    "suggesting growth", "appears to be",
])

_STAGE_ORDER = ["seed", "series a", "series b", "series c", "series d",
                "series e", "private equity", "public", "ipo"]

_VALID_INTENT_CATS = frozenset({"funding", "hiring", "transformation",
                                "expansion", "evaluation", "conference", "general"})

def _detect_icp_intent_category_simple(ctx: Dict) -> Tuple[str, List[str]]:
    intent_signals = ctx.get("intent_signals") or []
    prompt = ctx.get("prompt", "")
    combined = (" ".join(str(s) for s in intent_signals) + " " + prompt).lower()

    best_cat = "general"
    best_score = 0
    for cat, keywords in _ICP_INTENT_CATEGORIES.items():
        score = sum(1 for kw in keywords if kw in combined)
        if score > best_score:
            best_score = score
            best_cat = cat

    action_kw = _INTENT_ACTION_KEYWORDS.get(best_cat, list(SPECIFIC_INTENT_KW))
    return best_cat, action_kw

_llm_intent_cat_cache: Dict[str, Tuple[str, List[str]]] = {}

def _detect_icp_intent_category(ctx: Dict, proxy_url: str = "") -> Tuple[str, List[str]]:
    prompt_text = _s(ctx.get("prompt", ""))
    intent_signals = ctx.get("intent_signals") or []
    product_service = _s(ctx.get("product_service", ""))

    cache_key = f"ic|{prompt_text[:200]}|{product_service}|{'|'.join(str(s) for s in intent_signals[:3])}"
    if cache_key in _llm_intent_cat_cache:
        return _llm_intent_cat_cache[cache_key]

    fallback = _detect_icp_intent_category_simple(ctx)

    if not proxy_url:
        _llm_intent_cat_cache[cache_key] = fallback
        return fallback

    industry = _s(ctx.get("industry", ""))
    sub_industry = _s(ctx.get("sub_industry", ""))
    target_roles = ctx.get("target_roles") or []
    sig_str = ", ".join(str(s) for s in intent_signals[:5]) if intent_signals else "none"
    roles_str = ", ".join(str(r) for r in target_roles[:3]) if target_roles else "not specified"

    llm_prompt = f"""Classify this B2B sales lead qualification request into ONE intent category and provide action keywords.

ICP PROMPT: {prompt_text[:500]}
INTENT SIGNALS: {sig_str}
PRODUCT/SERVICE: {product_service or 'not specified'}
INDUSTRY: {industry or 'not specified'}
SUB-INDUSTRY: {sub_industry or 'not specified'}
TARGET ROLES: {roles_str}

CATEGORIES (pick exactly one):
- funding: Company recently raised capital, closed a funding round, got investment
- hiring: Company is actively recruiting, growing team, adding headcount
- transformation: Company undergoing digital transformation, cloud migration, tech adoption
- expansion: Company expanding to new markets, geographies, opening new offices
- evaluation: Company evaluating/replacing tools, running procurement, selecting vendors
- conference: Company leadership speaking at or attending industry events
- general: No clear single category fits, or multiple signals present

Respond with ONLY a JSON object, no markdown:
{{"category": "one_of_the_above", "action_keywords": ["10-15 specific lowercase verbs and phrases to search for on company websites and news articles that would prove this intent"]}}

The action_keywords should be specific to THIS ICP. For example:
- If the ICP mentions "hiring DevOps engineers", include "devops", "engineer", "hiring", "job posting", "open position", "careers"
- If the ICP mentions "evaluating CRM solutions", include "crm", "salesforce", "hubspot", "evaluating", "implemented", "selected", "migrating"
- Always include industry-specific terms relevant to the product/service"""

    try:
        resp = _openrouter_chat(llm_prompt, proxy_url)
        if resp:
            resp = resp.strip()
            if resp.startswith("```"):
                resp = re.sub(r'^```(?:json)?\s*', '', resp)
                resp = re.sub(r'\s*```$', '', resp)
            parsed = json.loads(resp)
            if isinstance(parsed, dict):
                cat = str(parsed.get("category", "general")).lower().strip()
                if cat not in _VALID_INTENT_CATS:
                    cat = fallback[0]
                raw_kw = parsed.get("action_keywords", [])
                if isinstance(raw_kw, list) and len(raw_kw) >= 3:
                    action_kw = [str(k).strip().lower() for k in raw_kw
                                 if isinstance(k, str) and 2 <= len(k.strip()) <= 50]
                    base_kw = _INTENT_ACTION_KEYWORDS.get(cat, list(SPECIFIC_INTENT_KW))
                    merged = list(dict.fromkeys(action_kw + base_kw))
                    result = (cat, merged)
                    _llm_intent_cat_cache[cache_key] = result
                    return result
    except Exception:
        pass

    _llm_intent_cat_cache[cache_key] = fallback
    return fallback

def _is_weak_page(url: str, text: str) -> bool:
    ul = url.lower()
    for pat in _WEAK_PAGE_URL_PATTERNS:
        if re.search(pat, ul):
            return True
    tl = text.lower()[:1500]
    hits = sum(1 for p in _WEAK_CONTENT_SIGNALS if p in tl)
    return hits >= 2

def _check_intent_contradiction(ev_text: str, ctx: Dict,
                                 icp_intent_cat: str) -> bool:
    el = ev_text.lower()
    icp_stage = (ctx.get("company_stage") or "").lower().strip()
    if icp_stage and icp_stage in _STAGE_ORDER:
        icp_idx = _STAGE_ORDER.index(icp_stage)
        for stage in _STAGE_ORDER:
            if stage != icp_stage and stage in el:
                ev_idx = _STAGE_ORDER.index(stage)
                if abs(ev_idx - icp_idx) >= 2:
                    return True
    if icp_intent_cat == "conference":
        if any(p in el for p in ("biography", "bio ", "about the author",
                                  "meet our", "leadership team")):
            return True
    if icp_intent_cat == "evaluation":
        generic = sum(1 for p in ("what is", "how to", "guide to",
                                   "introduction to", "beginner",
                                   "101", "explained") if p in el)
        if generic >= 2:
            return True
    if icp_intent_cat == "funding":
        if any(p in el for p in ("no funding", "bootstrap", "self-funded")):
            return True
    return False

def _snippet_concreteness_score(text: str, company: str) -> float:
    tl = text.lower()
    sc = 0.0
    if company and company.lower() in tl:
        sc += 0.25
    action_verbs = [
        "hired", "hiring", "raised", "closed", "launched", "announced",
        "released", "expanded", "partnered", "selected", "deployed",
        "implemented", "migrated", "acquired", "opened", "entered",
        "secured", "evaluated", "replaced", "spoke", "presented",
    ]
    if any(v in tl for v in action_verbs):
        sc += 0.25
    if re.search(r'\$[\d,.]+|[\d,.]+ (?:million|billion|mn|bn)', tl):
        sc += 0.15
    if re.search(r'\d{4}-\d{2}-\d{2}', text):
        sc += 0.10
    if re.search(r'(?:series [a-e]|seed|round|pre-seed)', tl):
        sc += 0.10
    if len(text) > 100:
        sc += 0.05
    if len(text) > 200:
        sc += 0.05
    vague_hits = sum(1 for p in _VAGUE_PHRASES if p in tl)
    sc -= vague_hits * 0.15
    return max(0.0, min(1.0, sc))

def _evidence_icp_relevance(ev: Dict, icp_intent_cat: str,
                             icp_action_kw: List[str],
                             company: str) -> float:
    text = (ev.get("extracted_text") or "").lower()
    if not text:
        return 0.0
    sc = 0.0
    action_hits = sum(1 for kw in icp_action_kw if kw in text)
    sc += min(0.4, action_hits * 0.12)
    concrete = _snippet_concreteness_score(ev.get("extracted_text", ""), company)
    sc += concrete * 0.3
    sig_date = ev.get("extracted_date", "")
    src = ev.get("source", "other")
    if sig_date:
        try:
            d = date.fromisoformat(sig_date)
            age_days = (date.today() - d).days
            if age_days <= 60:
                sc += 0.20
            elif age_days <= 365:
                sc += 0.10
            else:
                sc += 0.02
        except Exception:
            sc += 0.02
    src_mult = SOURCE_MULTIPLIERS.get(src, 0.3)
    sc += src_mult * 0.10
    return max(0.0, min(1.0, sc))

def _filter_and_rank_evidence(evidence: List[Dict], ctx: Dict,
                               icp_intent_cat: str,
                               icp_action_kw: List[str],
                               company: str) -> List[Dict]:
    filtered: List[Dict] = []
    for ev in evidence:
        url = ev.get("url", "")
        text = ev.get("extracted_text", "")
        if _is_weak_page(url, text):
            continue
        if _check_intent_contradiction(text, ctx, icp_intent_cat):
            continue
        filtered.append(ev)
    if not filtered:
        return evidence[:5]

    scored: List[Tuple[float, Dict]] = []
    for ev in filtered:
        rel = _evidence_icp_relevance(ev, icp_intent_cat, icp_action_kw, company)
        scored.append((rel, ev))
    scored.sort(key=lambda x: -x[0])
    return [ev for _, ev in scored]

OUTPUT_FIELDS = {
    "lead_id", "business", "company_linkedin", "company_website",
    "employee_count", "industry", "sub_industry", "country", "city",
    "state", "role", "role_type", "seniority", "intent_signals",
}

_page_cache: Dict[str, Optional[str]] = {}
_desearch_date_cache: Dict[str, str] = {}
_last_modified_cache: Dict[str, str] = {}
_run_proxy_url: str = ""

def _s(v: Any) -> str:
    return str(v or "").strip()

def _sl(v: Any) -> str:
    return _s(v).lower()

def _html_to_text(html: str) -> str:
    if not html:
        return ""
    t = re.sub(r'<script[^>]*>.*?</script>', ' ', html,
               flags=re.DOTALL | re.IGNORECASE)
    t = re.sub(r'<style[^>]*>.*?</style>', ' ', t,
               flags=re.DOTALL | re.IGNORECASE)
    t = re.sub(r'<[^>]+>', ' ', t)
    try:
        import html as _html_mod
        t = _html_mod.unescape(t)
    except Exception:
        t = re.sub(r'&#x[0-9a-fA-F]+;', ' ', t)
        t = re.sub(r'&#\d+;', ' ', t)
        t = re.sub(r'&[a-z]+;', ' ', t)
    t = re.sub(r'\s+', ' ', t)
    return t.strip()

def _proxy_get(url: str, timeout: float = _HTTP_TIMEOUT) -> Optional[str]:
    try:
        with httpx.Client(timeout=timeout) as c:
            r = c.get(url)
            if r.status_code == 200 and len(r.text) > 100:
                return r.text
    except Exception:
        pass
    return None

def _proxy_get_json(url: str, timeout: float = 5.0) -> Optional[Any]:
    try:
        with httpx.Client(timeout=timeout) as c:
            r = c.get(url)
            if r.status_code == 200:
                return r.json()
    except Exception:
        pass
    return None

def _cache_desearch_dates(results: List[Dict]) -> None:
    pass

def _desearch(query: str, proxy_url: str,
              timeout: float = 5.0) -> List[Dict]:
    """
    Web search via proxy.  Tries Desearch first (fast, usually <1s);
    if it returns no results, falls back to ScrapingDog's Google
    Search API which returns organic_results with {link, title, snippet}.
    """
    if not proxy_url:
        return []
    base = proxy_url.rstrip("/")
    endpoint = f"{base}/desearch/web?query={quote(query)}"
    resp = _proxy_get_json(endpoint, timeout=min(timeout, 3.0))
    if resp and isinstance(resp, dict):
        data = resp.get("data", [])
        if isinstance(data, list) and data:
            _cache_desearch_dates(data)
            return data
    endpoint2 = f"{base}/scrapingdog/google?query={quote(query)}"
    resp2 = _proxy_get_json(endpoint2, timeout=timeout)
    if resp2 and isinstance(resp2, dict):
        organic = resp2.get("organic_results", [])
        if isinstance(organic, list):
            _cache_desearch_dates(organic)
            results: List[Dict] = []
            for item in organic:
                results.append({
                    "url": item.get("link", ""),
                    "title": item.get("title", ""),
                    "snippet": item.get("snippet", ""),
                })
            return results
    return []

def _fetch_google_jobs(query: str, proxy_url: str,
                       country: str = "us", timeout: float = 3.0) -> List[Dict]:
    if not proxy_url or not query:
        return []
    ep = (f"{proxy_url.rstrip('/')}/scrapingdog/google_jobs"
          f"?query={quote(query)}&country={quote(country)}")
    try:
        r = _proxy_get_json(ep, timeout=timeout)
        return (r.get("jobs_results") or []) if isinstance(r, dict) else []
    except Exception:
        return []

def _fetch_linkedin_jobs(field: str, proxy_url: str,
                         timeout: float = 3.0) -> List[Dict]:
    if not proxy_url or not field:
        return []
    ep = f"{proxy_url.rstrip('/')}/scrapingdog/jobs?field={quote(field)}"
    try:
        r = _proxy_get_json(ep, timeout=timeout)
        if isinstance(r, list):
            return r
        return (r.get("jobs") or r.get("results") or []) if isinstance(r, dict) else []
    except Exception:
        return []

def _fetch_duckduckgo(query: str, proxy_url: str,
                      timeout: float = 3.0) -> List[Dict]:
    if not proxy_url or not query:
        return []
    ep = (f"{proxy_url.rstrip('/')}/scrapingdog/duckduckgo/search"
          f"?query={quote(query)}&df=m")
    try:
        r = _proxy_get_json(ep, timeout=timeout)
        return (r.get("organic_results") or []) if isinstance(r, dict) else []
    except Exception:
        return []

def _fetch_linkedin_company(linkedin_url: str, proxy_url: str) -> Optional[str]:
    cache_key = f"linkedin_profile|{linkedin_url}"
    if cache_key in _page_cache:
        return _page_cache[cache_key]
    m = re.search(r'linkedin\.com/company/([^/?]+)', linkedin_url, re.I)
    if not m:
        _page_cache[cache_key] = None
        return None
    company_id = m.group(1).strip("/")
    if not company_id:
        _page_cache[cache_key] = None
        return None
    result: Optional[str] = None
    if proxy_url:
        endpoint = (f"{proxy_url.rstrip('/')}/scrapingdog/profile"
                    f"?type=company&id={quote(company_id)}")
        result = _proxy_get(endpoint, timeout=5.0)
    _page_cache[cache_key] = result
    return result

def _scrape(url: str, proxy_url: str, dynamic: bool = False) -> Optional[str]:
    cache_key = f"{url}|{dynamic}"
    if cache_key in _page_cache:
        return _page_cache[cache_key]
    html: Optional[str] = None
    if proxy_url:
        dyn = "true" if dynamic else "false"
        endpoint = (f"{proxy_url.rstrip('/')}/scrapingdog/scrape"
                    f"?url={quote(url)}&dynamic={dyn}")
        try:
            with httpx.Client(timeout=_SCRAPE_TIMEOUT) as c:
                r = c.get(endpoint)
                if r.status_code == 200 and len(r.text) > 100:
                    html = r.text
                    lm = r.headers.get("last-modified") or r.headers.get("Last-Modified")
                    if lm:
                        dt = _parse_iso_to_date(lm[:16])
                        if not dt:
                            try:
                                from email.utils import parsedate_to_datetime
                                dt_obj = parsedate_to_datetime(lm).date()
                                dt = _validate_date_obj(dt_obj.year, dt_obj.month, dt_obj.day)
                            except Exception:
                                pass
                        if dt and url not in _last_modified_cache:
                            _last_modified_cache[url] = dt.isoformat()
        except Exception:
            pass
    _page_cache[cache_key] = html
    return html

def _fetch_wikipedia(company: str) -> Optional[Dict]:
    if not company or len(company) < 2:
        return None
    cache_key = f"wiki|{company}"
    if cache_key in _page_cache:
        cached = _page_cache[cache_key]
        if cached is None:
            return None
        return {"url": cached, "title": company, "text": ""}

    search_queries = [company]
    if len(company.split()) <= 3:
        search_queries.append(f"{company} company")
    search_titles_all: List[str] = []
    search_urls_all: List[str] = []
    seen_urls: set = set()

    for sq in search_queries:
        try:
            search_url = (
                "https://en.wikipedia.org/w/api.php"
                f"?action=opensearch&search={quote(sq)}"
                "&limit=5&format=json"
            )
            with httpx.Client(timeout=3.5) as c:
                r = c.get(search_url, headers={
                    "User-Agent": "LeadPoet-Qualification/1.0"})
                if r.status_code != 200:
                    continue
                data = r.json()
                if (not data or len(data) < 4
                        or not data[3] or not data[1]):
                    continue
                for t, u in zip(data[1], data[3]):
                    if u not in seen_urls:
                        seen_urls.add(u)
                        search_titles_all.append(t)
                        search_urls_all.append(u)
        except Exception:
            continue

    if not search_urls_all:
        _page_cache[cache_key] = None
        return None

    comp_lower = company.lower()
    comp_words = [w for w in comp_lower.split() if len(w) > 2]

    for idx in range(min(len(search_urls_all), 3)):
        article_url = search_urls_all[idx]
        article_title = search_titles_all[idx]
        try:
            with httpx.Client(timeout=3.0) as c:
                r = c.get(article_url, headers={
                    "User-Agent": "LeadPoet-Qualification/1.0"},
                    follow_redirects=True)
                if r.status_code != 200 or len(r.text) < 300:
                    continue
                text = _html_to_text(r.text)
                if len(text) < 100:
                    continue
                tl = text[:3000].lower()
                title_lower = article_title.lower()
                if ("(disambiguation)" in title_lower
                        or "may refer to" in tl
                        or "disambiguation" in tl):
                    continue
                if comp_words and not any(w in tl for w in comp_words):
                    continue
                _page_cache[cache_key] = article_url
                return {"url": article_url, "title": article_title,
                        "text": text}
        except Exception:
            continue

    _page_cache[cache_key] = None
    return None

_SENOO_LLM_PRIMARY = "openai/gpt-4o-mini"
_SENOO_LLM_FALLBACK = "meta-llama/llama-3.1-8b-instruct"


def _openrouter_chat(prompt: str, proxy_url: str,
                     model: str = "gpt-4o-mini") -> Optional[str]:
    if not proxy_url:
        return None
    endpoint = f"{proxy_url.rstrip('/')}/openrouter/chat/completions"
    # Prefer gpt-4o-mini; pass raw model name if it already has a provider prefix
    resolved = model if "/" in model else f"openai/{model}"
    for attempt_model in [resolved, _SENOO_LLM_FALLBACK]:
        try:
            with httpx.Client(timeout=_LLM_TIMEOUT) as c:
                r = c.post(endpoint, json={
                    "model": attempt_model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.2,
                    "max_tokens": 350,
                })
                if r.status_code == 200:
                    data = r.json()
                    if "choices" in data:
                        return data["choices"][0]["message"]["content"]
        except Exception:
            pass
    return None

_US_STATE_NAMES = (
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "district of columbia", "florida", "georgia",
    "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky",
    "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada", "new hampshire",
    "new jersey", "new mexico", "new york", "north carolina", "north dakota",
    "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
    "washington", "west virginia", "wisconsin", "wyoming",
)

def _extract_state_and_country_from_geography(geography: str) -> Tuple[str, str]:
    """
    Parse geography in icp_generator format: "Country, Region".
    E.g. "United States, Wisconsin" -> ("Wisconsin", "United States")
         "United Kingdom, London" -> ("", "United Kingdom")
         "Arizona" -> ("Arizona", "United States")
         "Hong Kong" -> ("", "Hong Kong")
         "Singapore" -> ("", "Singapore")
    """
    if not geography:
        return "", ""
    if "," in geography:
        parts = [p.strip() for p in geography.split(",", 1)]
        country = parts[0] or ""
        region = (parts[1] or "").strip()
        state = ""
        if country and "united states" in country.lower() and region:
            reg_lower = region.lower()
            if reg_lower in _US_STATE_NAMES:
                state = region.title() if region else ""
        return state, country
    geo_lower = geography.strip().lower()
    if geo_lower in _US_STATE_NAMES:
        return geography.strip().title(), "United States"
    # Standalone non-US-state value: treat as country (e.g., "Hong Kong", "Singapore")
    if geo_lower:
        return "", geography.strip()
    return "", ""


def _is_country_only_geography(geography: str) -> bool:
    if not geography or not geography.strip():
        return False
    geo = geography.strip()
    if "," in geo:
        region = geo.split(",", 1)[1].strip()
        return not region
    return geo.lower() not in _US_STATE_NAMES

def _extract_country_from_text(text: str) -> str:
    tl = text.lower()
    us_states = (
        "california", "texas", "new york", "florida", "illinois",
        "massachusetts", "washington", "colorado", "georgia", "virginia",
        "north carolina", "ohio", "pennsylvania", "michigan", "arizona",
        "new jersey", "oregon", "connecticut", "maryland", "minnesota",
        "utah", "tennessee", "indiana", "missouri", "wisconsin",
        "nevada", "iowa", "nebraska", "kansas", "san francisco",
        "boston", "chicago", "los angeles", "seattle", "austin",
        "denver", "atlanta", "miami", "portland", "nashville",
    )
    if any(st in tl for st in us_states):
        return "United States"
    if re.search(r'\b(us|usa|u\.s\.|united states|america)\b', tl):
        return "United States"
    if re.search(r'\b(uk|united kingdom|britain|london)\b', tl):
        return "United Kingdom"
    if re.search(r'\b(canada|toronto|vancouver)\b', tl):
        return "Canada"
    if re.search(r'\b(germany|berlin|munich)\b', tl):
        return "Germany"
    if re.search(r'\b(india|bangalore|mumbai)\b', tl):
        return "India"
    if re.search(r'\b(australia|sydney|melbourne)\b', tl):
        return "Australia"
    return ""

def _extract_state_from_text(text: str) -> str:
    tl = text.lower()
    for state in _US_STATE_NAMES:
        if state in tl:
            return state.title()
    return ""

_geo_llm_cache: Dict[str, Tuple[str, str, str]] = {}

def _extract_geography_via_llm(geography: str, proxy_url: str) -> Tuple[str, str, str]:
    if not geography or not proxy_url:
        return "", "", ""
    cache_key = geography.strip().lower()
    if cache_key in _geo_llm_cache:
        return _geo_llm_cache[cache_key]

    prompt = (
        "Extract the country, state/region, and city from this geography string.\n"
        f"Geography: {geography}\n\n"
        "Return ONLY a JSON object:\n"
        '{"country": "full country name", "state": "state or region name, or empty", "city": "city name, or empty"}\n\n'
        "Rules:\n"
        '- Use the full official country name (e.g., "United States", "United Kingdom")\n'
        '- For US states, use the full state name (e.g., "California", not "CA")\n'
        "- If no state/region is specified, return empty string for state\n"
        "- If no city is specified, return empty string for city\n"
        "- If the geography is just a country, return country only\n"
        "- If the geography is a city name, infer the country and state\n"
        "Respond with ONLY the JSON, no markdown."
    )
    resp = _openrouter_chat(prompt, proxy_url)
    if not resp:
        _geo_llm_cache[cache_key] = ("", "", "")
        return "", "", ""
    try:
        cleaned = resp.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)
        parsed = json.loads(cleaned)
        country = _s(parsed.get("country", ""))
        state = _s(parsed.get("state", ""))
        city = _s(parsed.get("city", ""))
        _geo_llm_cache[cache_key] = (country, state, city)
        return country, state, city
    except Exception:
        _geo_llm_cache[cache_key] = ("", "", "")
        return "", "", ""

def _extract_industry_from_text(text: str) -> str:
    tl = text.lower()
    industry_map = [
        (r'\b(saas|software|devtools?|dev tools?)\b', "Information Technology"),
        (r'\b(fintech|financial technology|payments?)\b', "Financial Services"),
        (r'\b(cybersecurity|cyber security|infosec|security)\b', "Information Technology"),
        (r'\b(healthcare|health\s*care|health\s*tech|medtech)\b', "Health Care"),
        (r'\b(e-?commerce|ecommerce|retail tech)\b', "Retail"),
        (r'\b(ai|artificial intelligence|machine learning|ml)\b', "Information Technology"),
        (r'\b(cloud|cloud infrastructure)\b', "Information Technology"),
        (r'\b(hr\s*tech|workforce management|human resources)\b', "Professional Services"),
        (r'\b(logistics|supply chain)\b', "Transportation"),
        (r'\b(marketing tech|martech|marketing automation)\b', "Information Technology"),
        (r'\b(edtech|education technology)\b', "Education"),
        (r'\b(biotech|biotechnology)\b', "Health Care"),
        (r'\b(cleantech|clean energy|renewable)\b', "Energy"),
        (r'\b(real estate|proptech)\b', "Real Estate"),
        (r'\b(data analytics|analytics)\b', "Information Technology"),
        (r'\b(enterprise software)\b', "Information Technology"),
        (r'\b(insurance|insurtech)\b', "Financial Services"),
    ]
    for pattern, industry in industry_map:
        if re.search(pattern, tl):
            return industry
    return ""

def _extract_sub_industry_from_text(text: str) -> str:
    tl = text.lower()
    sub_map = [
        (r'\b(saas|software as a service)\b', "SaaS"),
        (r'\b(fintech)\b', "Fintech"),
        (r'\b(cybersecurity|cyber security)\b', "Cybersecurity"),
        (r'\b(devtools?|developer tools?)\b', "Developer Tools"),
        (r'\b(e-?commerce)\b', "E-Commerce"),
        (r'\b(ai|artificial intelligence)\b', "Artificial Intelligence"),
        (r'\b(cloud infrastructure|cloud computing)\b', "Cloud Computing"),
        (r'\b(payments?)\b', "Payments"),
        (r'\b(hr\s*tech)\b', "HR Technology"),
        (r'\b(data analytics|analytics)\b', "Data Analytics"),
        (r'\b(marketing automation|martech)\b', "Marketing Technology"),
        (r'\b(supply chain)\b', "Supply Chain"),
        (r'\b(insurance|insurtech)\b', "Insurance Technology"),
        (r'\b(health\s*tech|medtech)\b', "Health Technology"),
    ]
    for pattern, sub in sub_map:
        if re.search(pattern, tl):
            return sub
    return ""

def _extract_roles_from_text(text: str) -> List[str]:
    roles: List[str] = []
    role_patterns = [
        r'\b(VP of Sales|VP Sales)\b',
        r'\b(VP of Marketing|VP Marketing)\b',
        r'\b(VP of Engineering|VP Engineering)\b',
        r'\b(VP of Customer Success)\b',
        r'\b(VP of Partnerships)\b',
        r'\b(Head of (?:Sales|Marketing|Growth|Revenue Operations|'
        r'Revenue|Engineering|Product|Partnerships|'
        r'Business Development|Customer Success|'
        r'Sales Operations|People|Talent|Design))\b',
        r'\b(Chief (?:Executive|Technology|Financial|Marketing|Operating|'
        r'Revenue|Information Security) Officer)\b',
        r'\b(C[ETFMOI]O|CRO|CISO)\b',
        r'\b(Director of (?:Sales|Marketing|Engineering|Product|'
        r'Business Development|Operations|Revenue))\b',
        r'\b(Sales Director|Marketing Director|Engineering Director)\b',
        r'\b(Sales Operations Manager)\b',
        r'\b(Marketing Operations (?:Lead|Manager))\b',
        r'\b(Revenue (?:Leader|Manager))\b',
    ]
    for pat in role_patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            role = m.group(1).strip()
            if role and role not in roles:
                roles.append(role)
    return roles

def _extract_seniority_from_text(text: str) -> str:
    tl = text.lower()
    if re.search(r'\b(ceo|cto|cfo|cmo|coo|cro|ciso|chief)\b', tl):
        return "C-Suite"
    if re.search(r'\b(vp|vice president)\b', tl):
        return "VP"
    if re.search(r'\b(director|head of)\b', tl):
        return "Director"
    if re.search(r'\b(manager|lead)\b', tl):
        return ROLE_TYPE_SENIORITY.get("Manager", "Manager")
    return ""

def _extract_employee_count_from_text(text: str) -> str:
    m = re.search(r'(\d[\d,]*)\s*[-–]\s*(\d[\d,]*)\s*employees?', text, re.I)
    if m:
        return f"{m.group(1).replace(',','')}-{m.group(2).replace(',','')}"
    tl = text.lower()
    if re.search(r'\bseries\s*[ab][-–]c\b', tl):
        return "11-500"
    if re.search(r'\bseries\s*[ab][-–]d\b', tl):
        return "11-1000"
    if re.search(r'\bseries\s*b\s*(?:or|\+|and)\s*(?:later|above|beyond)\b', tl):
        return "51-5000"
    if re.search(r'\bseed\b', tl):
        return "1-10"
    if re.search(r'\bseries\s*a\b', tl):
        return "11-50"
    if re.search(r'\bseries\s*b\b', tl):
        return "51-200"
    if re.search(r'\bseries\s*c\b', tl):
        return "201-500"
    if re.search(r'\bseries\s*d\b', tl):
        return "501-1000"
    if re.search(r'\bstartups?\b', tl):
        return "11-200"
    if re.search(r'\blate[- ]stage\b', tl):
        return "501-10000"
    if re.search(r'\benterprise\b', tl):
        return "1001-10000"
    if re.search(r'\bfortune\s*500\b', tl):
        return "10001+"
    if re.search(r'\bpublic\b', tl):
        return "1001-10000"
    return ""

def _extract_domain_words(ctx: Dict) -> Set[str]:
    """
    Extract domain keywords from ICP for description overlap scoring.
    E.g. product_service='CRM software' -> {'crm', 'software'},
    sub_industry='Payments' -> {'payments'}, intent_signals may add 'hiring', etc.
    Used for small boost when lead description overlaps with ICP domain words.
    """
    domain: Set[str] = set()
    stop = {"the", "and", "for", "with", "software", "platform", "tool", "system",
            "solution", "services", "management", "company", "solution"}
    for src in (
        ctx.get("product_service"),
        ctx.get("sub_industry"),
        ctx.get("industry"),
    ):
        if not src:
            continue
        for w in re.findall(r'\w{2,}', _sl(src)):
            if w not in stop:
                domain.add(w)
    for sig in (ctx.get("intent_signals") or []):
        if isinstance(sig, str):
            for w in re.findall(r'\w{2,}', _sl(sig)):
                if w not in stop and len(w) > 2:
                    domain.add(w)
    return domain

def _parse_icp(icp: Dict[str, Any]) -> Dict[str, Any]:
    geography = _s(icp.get("geography"))
    state_from_geo, country_from_geo = _extract_state_and_country_from_geography(geography)

    ctx: Dict[str, Any] = {
        "industry": _s(icp.get("industry")),
        "sub_industry": _s(icp.get("sub_industry")),
        "country": _s(icp.get("country")) or country_from_geo,
        "state": state_from_geo or _s(icp.get("state")),
        "city": _s(icp.get("city")),
        "target_roles": [_s(r) for r in (icp.get("target_roles") or []) if _s(r)],
        "target_seniority": _s(icp.get("target_seniority")),
        "employee_count": _s(icp.get("employee_count") or icp.get("company_size")),
        "company_stage": _s(icp.get("company_stage")),
        "product_service": _s(icp.get("product_service")),
        "geography": geography,
        "intent_signals": [_s(s) for s in (icp.get("intent_signals") or []) if isinstance(s, str) and _s(s).strip()],
    }
    prompt = _s(icp.get("prompt") or icp.get("buyer_description", ""))
    ctx["prompt"] = prompt

    if not ctx["country"] and geography:
        ctx["country"] = country_from_geo or _extract_country_from_text(geography)
    if not ctx["country"] and prompt:
        ctx["country"] = _extract_country_from_text(prompt)
    if not ctx["state"] and prompt:
        ctx["state"] = _extract_state_from_text(prompt)
    if not ctx["city"] and geography:
        geo_lower = geography.strip().lower()
        for city_name in ("san francisco", "new york", "los angeles", "chicago",
                          "boston", "seattle", "austin", "denver", "atlanta",
                          "miami", "portland", "nashville", "dallas", "houston",
                          "phoenix", "philadelphia", "london", "toronto",
                          "vancouver", "sydney", "melbourne", "berlin", "munich",
                          "bangalore", "mumbai", "singapore", "hong kong"):
            if city_name in geo_lower:
                ctx["city"] = city_name.title()
                break
    if not ctx["industry"] and prompt:
        ctx["industry"] = _extract_industry_from_text(prompt)
    if not ctx["sub_industry"] and prompt:
        ctx["sub_industry"] = _extract_sub_industry_from_text(prompt)
    if not ctx["target_roles"] and prompt:
        ctx["target_roles"] = _extract_roles_from_text(prompt)
    if not ctx["target_seniority"] and prompt:
        ctx["target_seniority"] = _extract_seniority_from_text(prompt)
    if not ctx["employee_count"] and prompt:
        ctx["employee_count"] = _extract_employee_count_from_text(prompt)
    if not ctx["product_service"] and prompt:
        ctx["product_service"] = _extract_product_from_text(prompt)

    ctx["domain_words"] = _extract_domain_words(ctx)
    return ctx

def _extract_product_from_text(text: str) -> str:
    tl = text.lower()
    product_map = [
        (r'\b(crm|salesforce|hubspot)\b', "CRM"),
        (r'\b(payments?|payment processing)\b', "Payments"),
        (r'\b(data platform|analytics|bi)\b', "Data Platform"),
        (r'\b(devops|cloud)\b', "DevOps"),
        (r'\b(hr|human resources|workforce)\b', "HR"),
        (r'\b(cybersecurity|security)\b', "Cybersecurity"),
        (r'\b(e-?commerce|retail)\b', "E-Commerce"),
    ]
    for pattern, product in product_map:
        if re.search(pattern, tl):
            return product
    return ""

_QUERY_LIMIT = 80000  # cap total DB rows to keep memory reasonable

_KNOWN_EMP_BANDS: List[Tuple[str, int, int]] = [
    ("1-10", 1, 10), ("2-10", 2, 10),
    ("11-50", 11, 50),
    ("51-200", 51, 200),
    ("201-500", 201, 500),
    ("501-1000", 501, 1000), ("501-1,000", 501, 1000),
    ("1001-5000", 1001, 5000), ("1,001-5,000", 1001, 5000),
    ("5001-10000", 5001, 10000), ("5,001-10,000", 5001, 10000),
    ("10001+", 10001, 10**8), ("10,001+", 10001, 10**8),
    ("10000+", 10000, 10**8), ("10,000+", 10000, 10**8),
    ("1000+", 1000, 10**8), ("1,000+", 1000, 10**8),
    ("5000+", 5000, 10**8), ("5,000+", 5000, 10**8),
]

_SELECT_COLS = (
    "id,business,website,company_linkedin,role,role_type,"
    "industry,sub_industry,country,city,state,"
    "employee_count,description"
)

def _parse_emp_range(s: str) -> Tuple[Optional[int], Optional[int]]:
    s = _s(s).replace(",", "").replace("\u2013", "-").replace("\u2014", "-")
    m = re.match(r'(\d+)\s*[-]\s*(\d+)', s)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.match(r'(\d+)\s*\+', s)
    if m:
        return int(m.group(1)), 10**8
    m = re.match(r'(\d+)', s)
    if m:
        v = int(m.group(1))
        return v, v
    return None, None

def _get_overlapping_bands(icp_emp: str) -> List[str]:
    lo, hi = _parse_emp_range(icp_emp)
    if lo is None:
        return []
    adj_lo = max(1, lo // 2)
    adj_hi = hi * 2 if hi < 10**7 else hi
    overlapping: List[str] = []
    seen: Set[str] = set()
    for band_text, band_lo, band_hi in _KNOWN_EMP_BANDS:
        if band_hi >= adj_lo and band_lo <= adj_hi:
            if band_text not in seen:
                seen.add(band_text)
                overlapping.append(band_text)
    return overlapping

def _get_allowed_role_types(ctx: Dict) -> List[str]:
    target_roles = ctx.get("target_roles") or []
    target_sen = _s(ctx.get("target_seniority"))
    allowed: Set[str] = set()
    for rt, sen in ROLE_TYPE_SENIORITY.items():
        if target_sen and sen == target_sen:
            allowed.add(rt)
        for tr in target_roles:
            tr_l = tr.lower()
            sen_l = sen.lower()
            rt_l = rt.lower()
            if sen_l in tr_l or rt_l in tr_l or any(w in rt_l for w in tr_l.split() if len(w) > 2):
                allowed.add(rt)
    return list(allowed) if allowed else []

def _build_fts_query(ctx: Dict) -> str:
    """
    Build combined text for Postgres FTS. Works with to_tsvector / plainto_tsquery
    or websearch_to_tsquery. search_content = industry + sub_industry + role +
    role_type + country + city + description. Space-separated terms for FTS.
    """
    parts: List[str] = []
    for k in ("industry", "sub_industry", "product_service"):
        v = ctx.get(k)
        if v and _s(v):
            parts.append(_s(v))
    for r in (ctx.get("target_roles") or []):
        if _s(r):
            parts.append(_s(r))
    if ctx.get("target_seniority"):
        parts.append(ctx["target_seniority"])
    if ctx.get("country"):
        parts.append(ctx["country"])
    if ctx.get("domain_words"):
        parts.extend(list(ctx["domain_words"])[:10])
    return " ".join(parts).strip() if parts else ""

_INDUSTRY_STOPWORDS = frozenset({
    "and", "of", "the", "for", "in", "on", "to", "a", "an", "or", "&",
})

def _extract_ilike_keywords(name: str) -> List[str]:
    words = re.split(r'[\s\-/&,]+', name)
    keywords = [w for w in words
                if len(w) > 2 and w.lower() not in _INDUSTRY_STOPWORDS]
    keywords.sort(key=len, reverse=True)
    return keywords

def _build_fuzzy_ilike_patterns(name: str) -> List[str]:
    patterns = [f"%{name}%"]
    no_space = name.replace(" ", "")
    if no_space != name:
        patterns.append(f"%{no_space}%")
    no_hyphen = name.replace("-", "")
    if no_hyphen not in (name, no_space):
        patterns.append(f"%{no_hyphen}%")
    no_hyphen_space = name.replace("-", " ")
    if no_hyphen_space != name:
        patterns.append(f"%{no_hyphen_space}%")
    kws = _extract_ilike_keywords(name)
    for kw in kws[:2]:
        p = f"%{kw}%"
        if p not in patterns:
            patterns.append(p)
    return patterns

def _query_db(db, table: str, ctx: Dict) -> List[Dict]:
    industry = ctx.get("industry") or ""
    sub_industry = ctx.get("sub_industry") or ""
    country = ctx.get("country") or ""
    results: List[Dict] = []
    seen_ids: Set[int] = set()

    if not table:
        print("[SENOO] _query_db: table name is empty!", flush=True)
        return []

    _PAGE_SIZE = 5000
    _DB_TIME_LIMIT = 5.0
    _db_t0 = time.time()

    def _execute_query(q, limit: int) -> int:
        total_fetched = 0
        offset = 0
        while offset < limit:
            if time.time() - _db_t0 > _DB_TIME_LIMIT:
                break
            page_end = min(offset + _PAGE_SIZE - 1, limit - 1)
            try:
                resp = q.range(offset, page_end).execute()
                batch = resp.data or []
                if not batch:
                    break
                for r in batch:
                    rid = r.get("id") or r.get("lead_id")
                    if rid is not None and rid not in seen_ids:
                        seen_ids.add(rid)
                        if r.get("id") is None and rid is not None:
                            r["id"] = rid
                        results.append(r)
                total_fetched += len(batch)
                if len(batch) < _PAGE_SIZE:
                    break
                offset += _PAGE_SIZE
            except Exception as e:
                print(f"[SENOO] DB query page failed at offset {offset}: {e}", flush=True)
                break
        return total_fetched

    def _base_query():
        q = db.table(table).select(_SELECT_COLS)
        try:
            q = q.or_("website.not.is.null,company_linkedin.not.is.null")
        except Exception:
            pass
        return q

    ind_patterns = _build_fuzzy_ilike_patterns(industry) if industry else []
    sub_patterns = _build_fuzzy_ilike_patterns(sub_industry) if sub_industry else []
    country_pat = f"%{country}%" if country else ""
    state = ctx.get("state") or ""
    state_pat = f"%{state}%" if state else ""

    def _db_budget():
        return _DB_TIME_LIMIT - (time.time() - _db_t0)

    def _run_industry_query(ind_p: str, sub_p: str,
                            c_pat: str = "", s_pat: str = ""):
        q = _base_query()
        q = q.ilike("industry", ind_p)
        if sub_p:
            q = q.ilike("sub_industry", sub_p)
        if c_pat:
            q = q.ilike("country", c_pat)
        if s_pat:
            q = q.ilike("state", s_pat)
        _execute_query(q, _QUERY_LIMIT)

    # Tier 0: exact patterns + country + state
    if ind_patterns and sub_patterns and country and state:
        _run_industry_query(ind_patterns[0], sub_patterns[0],
                            country_pat, state_pat)
        print(f"[SENOO] DB tier0 (exact+state): {len(results)}", flush=True)

    # Tier 0b: fuzzy industry patterns + sub + country + state
    if len(results) < 20 and ind_patterns and sub_patterns and country and state and _db_budget() > 0.5:
        for ip in ind_patterns[1:3]:
            if _db_budget() < 0.3 or len(results) >= 50:
                break
            for sp in sub_patterns[:2]:
                if _db_budget() < 0.3 or len(results) >= 50:
                    break
                _run_industry_query(ip, sp, country_pat, state_pat)
        print(f"[SENOO] DB tier0b (fuzzy+state): {len(results)}", flush=True)

    # Tier 1: exact patterns + country (no state)
    if len(results) < 20 and ind_patterns and sub_patterns and country and _db_budget() > 0.5:
        _run_industry_query(ind_patterns[0], sub_patterns[0], country_pat)
        print(f"[SENOO] DB tier1 (exact+country): {len(results)}", flush=True)

    # Tier 1b: fuzzy industry + sub + country
    if len(results) < 20 and ind_patterns and sub_patterns and country and _db_budget() > 0.5:
        for ip in ind_patterns[1:3]:
            if _db_budget() < 0.3 or len(results) >= 50:
                break
            for sp in sub_patterns[:2]:
                if _db_budget() < 0.3 or len(results) >= 50:
                    break
                _run_industry_query(ip, sp, country_pat)
        print(f"[SENOO] DB tier1b (fuzzy+country): {len(results)}", flush=True)

    # Tier 1.5: industry + country + state (drop sub)
    if len(results) < 20 and ind_patterns and country and state and _db_budget() > 0.5:
        for ip in ind_patterns[:2]:
            if _db_budget() < 0.3 or len(results) >= 50:
                break
            _run_industry_query(ip, "", country_pat, state_pat)
        print(f"[SENOO] DB tier1.5 (ind+country+state): {len(results)}", flush=True)

    # Tier 2: industry + country (drop sub and state)
    if len(results) < 20 and ind_patterns and country and _db_budget() > 0.5:
        for ip in ind_patterns[:2]:
            if _db_budget() < 0.3 or len(results) >= 50:
                break
            _run_industry_query(ip, "", country_pat)
        print(f"[SENOO] DB tier2 (ind+country): {len(results)}", flush=True)

    # Tier 3: industry only
    if not results and ind_patterns and _db_budget() > 0.5:
        for ip in ind_patterns[:2]:
            if _db_budget() < 0.3 or len(results) >= 50:
                break
            _run_industry_query(ip, "")
        print(f"[SENOO] DB tier3 (ind only): {len(results)}", flush=True)

    # Tier 4: no filters (last resort)
    if not results:
        try:
            q = db.table(table).select(_SELECT_COLS).limit(50)
            resp = q.execute()
            for r in (resp.data or []):
                rid = r.get("id") or r.get("lead_id")
                if rid is not None and rid not in seen_ids:
                    seen_ids.add(rid)
                    if r.get("id") is None and rid is not None:
                        r["id"] = rid
                    results.append(r)
            print(f"[SENOO] DB tier4 (fallback): {len(results)}", flush=True)
        except Exception as e:
            print(f"[SENOO] DB tier4 fallback failed: {e}", flush=True)

    # LOCAL fuzzy filter: keep only leads that pass the validator's thresholds
    icp_ind_lower = industry.lower().strip()
    icp_sub_lower = sub_industry.lower().strip()
    if icp_ind_lower:
        filtered = []
        for r in results:
            r_ind = _sl(r.get("industry", "")).strip()
            if not r_ind:
                continue
            if _fuzz.ratio(r_ind, icp_ind_lower) < 80:
                continue
            if icp_sub_lower:
                r_sub = _sl(r.get("sub_industry", "")).strip()
                if r_sub and _fuzz.ratio(r_sub, icp_sub_lower) < 70:
                    continue
            filtered.append(r)
        print(f"[SENOO] local fuzzy filter: {len(results)} -> {len(filtered)} "
              f"(ind={icp_ind_lower}, sub={icp_sub_lower})", flush=True)
        results = filtered

    for r in results:
        if r.get("id") is None and r.get("lead_id") is not None:
            r["id"] = r["lead_id"]
    return results[:_QUERY_LIMIT]

def _employee_overlap(lead_emp: str, icp_emp: str) -> float:
    def _parse(s):
        s = _s(s).replace(",", "").replace("\u2013", "-").replace("\u2014", "-")
        m = re.match(r'(\d+)\s*[-]\s*(\d+)', s)
        if m:
            return int(m.group(1)), int(m.group(2))
        m = re.match(r'(\d+)\s*\+', s)
        if m:
            return int(m.group(1)), 10**9
        m = re.match(r'(\d+)', s)
        if m:
            v = int(m.group(1))
            return v, v
        return None, None
    la, lb = _parse(lead_emp)
    ia, ib = _parse(icp_emp)
    if ia is None:
        return 0.5
    if la is None:
        return 0.0
    if lb < ia or la > ib:
        return 0.0
    return 1.0

_SEN_CSUITE = ROLE_TYPE_SENIORITY.get("Founder/Owner", "C-Suite")
_SEN_VP = ROLE_TYPE_SENIORITY.get("VP/SVP", "VP")
_SEN_DIR = ROLE_TYPE_SENIORITY.get("Director", "Director")
_SEN_MGR = ROLE_TYPE_SENIORITY.get("Manager", "Manager")
_SEN_DEFAULT = ROLE_TYPE_SENIORITY.get("Other",
                                        list(SENIORITY_ENUM)[-1])

def _infer_seniority(role: str, role_type: str = "") -> str:
    rt = _s(role_type)
    if rt in ROLE_TYPE_SENIORITY:
        return ROLE_TYPE_SENIORITY[rt]
    rl = _sl(role)
    if any(k in rl for k in ("chief ", " ceo", " cto", " cfo", " cmo",
                               " coo", " cro", "president", "founder")):
        return _SEN_CSUITE
    if re.search(r'\b(ceo|cto|cfo|cmo|coo|cro|ciso)\b', rl):
        return _SEN_CSUITE
    if re.search(r'\b(vp|vice president|svp|evp)\b', rl):
        return _SEN_VP
    if re.search(r'\b(director|head of)\b', rl):
        return _SEN_DIR
    if re.search(r'\b(manager|team lead|supervisor)\b', rl):
        return _SEN_MGR
    return _SEN_DEFAULT

def _infer_role_type(role: str) -> str:
    rl = _sl(role)
    if any(k in rl for k in ("engineer", "developer", "architect",
                               "cto", "devops", "software", "technical")):
        return "Engineer/Technical"
    if any(k in rl for k in ("sales", "account executive", "revenue",
                               "business development")):
        return "Sales"
    if any(k in rl for k in ("marketing", "cmo", "growth", "brand",
                               "demand gen")):
        return "Marketing"
    if any(k in rl for k in ("finance", "cfo", "controller")):
        return "Finance/Accounting"
    if any(k in rl for k in ("operations", "coo", "supply chain")):
        return "Operations"
    if any(k in rl for k in ("hr", "people", "talent", "recruiting")):
        return "HR/People"
    if any(k in rl for k in ("product manager", "product owner")):
        return "Product"
    if any(k in rl for k in ("compliance", "compliance officer", "vp compliance")):
        return "Compliance"
    if any(k in rl for k in ("risk", "chief risk", "vp risk")):
        return "Risk Management"
    if any(k in rl for k in ("ceo", "president", "founder",
                               "managing director")):
        return "C-Level Executive"
    return "Other"

def _decision_maker_score(lead: Dict, ctx: Dict) -> float:
    lead_role = _sl(lead.get("role", ""))
    lead_role_type = _sl(lead.get("role_type", ""))
    target_roles = ctx.get("target_roles") or []
    target_sen = ctx.get("target_seniority") or ""

    for pat in DECISION_MAKER_ANTI_PATTERNS:
        if pat in lead_role or pat in lead_role_type:
            return 0.0
    for gr in DECISION_MAKER_GENERIC_ROLES:
        if gr in lead_role:
            return 0.0

    lead_sen = _infer_seniority(lead.get("role", ""), lead.get("role_type", ""))
    lead_rank = SENIORITY_RANK.get(lead_sen, 1)
    target_rank = SENIORITY_RANK.get(target_sen, 3) if target_sen else 3

    sen_score = 1.0 if lead_rank >= target_rank else (0.5 + 0.1 * (lead_rank - 1))
    if target_sen and lead_rank < target_rank:
        sen_score = max(0.2, 0.5 - 0.1 * (target_rank - lead_rank))

    role_match = _role_synonym_match(lead_role, target_roles)

    rt_align = _role_type_dm_alignment(lead, ctx)

    return 0.4 * sen_score + 0.35 * role_match + 0.25 * rt_align

def _role_type_dm_alignment(lead: Dict, ctx: Dict) -> float:
    """
    Score 1.0 if lead role_type maps to ICP target_roles category OR seniority matches.
    Score 0.0 if clearly incompatible (e.g. ICP wants VP Sales but lead is Engineer/Technical).
    """
    lead_role_type = _s(lead.get("role_type"))
    lead_role = _s(lead.get("role", ""))
    lead_sen = _infer_seniority(lead_role, lead_role_type)
    target_roles = ctx.get("target_roles") or []
    target_sen = ctx.get("target_seniority") or ""
    if target_sen and lead_sen == target_sen:
        return 1.0
    if not target_roles and not target_sen:
        return 0.5
    lead_combined = _sl(lead_role_type or "") + " " + _sl(lead_role)
    for tr in target_roles:
        tr_lower = _sl(tr)
        if _fuzz.partial_ratio(lead_combined, tr_lower) >= 60:
            return 1.0
        if lead_role_type and _fuzz.partial_ratio(_sl(lead_role_type), tr_lower) >= 60:
            return 1.0
    if target_roles and lead_role_type:
        tr_cats: Set[str] = set()
        for tr in target_roles:
            tr_lower = _sl(tr)
            if "sales" in tr_lower or "revenue" in tr_lower or "bd " in tr_lower:
                tr_cats.add("sales")
            if "marketing" in tr_lower or "cmo" in tr_lower or "growth" in tr_lower:
                tr_cats.add("marketing")
            if "engineer" in tr_lower or "technical" in tr_lower or "cto" in tr_lower or "developer" in tr_lower:
                tr_cats.add("engineer")
        lead_cat = _sl(lead_role_type)
        if tr_cats:
            lead_is_eng = "engineer" in lead_cat or "technical" in lead_cat
            lead_is_sales = "sales" in lead_cat or "revenue" in lead_cat
            lead_is_mkt = "marketing" in lead_cat
            want_eng = "engineer" in tr_cats
            want_sales = "sales" in tr_cats
            want_mkt = "marketing" in tr_cats
            if want_sales and not want_eng and lead_is_eng:
                return 0.0
            if want_eng and not want_sales and lead_is_sales:
                return 0.0
    return 0.5

def _domain_keyword_overlap(lead: Dict, domain_words: Set[str]) -> float:
    if not domain_words:
        return 0.5
    desc = _sl(lead.get("description", ""))
    ind = _sl(lead.get("industry", ""))
    sub = _sl(lead.get("sub_industry", ""))
    combined = f"{desc} {ind} {sub}"
    hits = sum(1 for w in domain_words if w in combined)
    return min(1.0, 0.5 + 0.5 * (hits / max(1, len(domain_words))))

def _extract_match_features(lead: Dict, ctx: Dict) -> Dict[str, float]:
    ind = _fuzz.ratio(_sl(lead.get("industry")),
                      _sl(ctx["industry"])) / 100.0
    sub = _fuzz.ratio(_sl(lead.get("sub_industry")),
                      _sl(ctx["sub_industry"])) / 100.0
    role_score = _role_synonym_match(_sl(lead.get("role")),
                                      ctx.get("target_roles") or [])
    if not ctx["target_roles"]:
        role_score = 0.5
    sen = 0.0
    lead_sen = _infer_seniority(_s(lead.get("role")),
                                 _s(lead.get("role_type")))
    target_sen = ctx.get("target_seniority")
    if target_sen and lead_sen == target_sen:
        sen = 1.0
    elif not target_sen:
        sen = 0.5
    elif lead_sen in ("VP", "Director") and target_sen in ("VP", "Director"):
        sen = 0.7

    icp_country = _sl(ctx.get("country", ""))
    icp_state = _sl(ctx.get("state", ""))
    icp_city = _sl(ctx.get("city", ""))
    lead_country = _sl(lead.get("country", ""))
    lead_state = _sl(lead.get("state", ""))
    lead_city = _sl(lead.get("city", ""))

    geo = 0.0
    if icp_country:
        country_match = (icp_country == lead_country
                         or icp_country in lead_country
                         or lead_country in icp_country)
        if not country_match:
            geo = 0.0
        elif icp_city and icp_state:
            state_match = _fuzz.ratio(lead_state, icp_state) >= 80
            city_match = (lead_city and (icp_city == lead_city
                          or _fuzz.ratio(lead_city, icp_city) >= 80))
            if city_match and state_match:
                geo = 1.0
            elif state_match:
                geo = 0.7
            elif city_match:
                geo = 0.65
            else:
                geo = 0.35
        elif icp_state:
            state_match = _fuzz.ratio(lead_state, icp_state) >= 80
            geo = 1.0 if state_match else 0.4
        elif icp_city:
            city_match = (lead_city and (icp_city == lead_city
                          or _fuzz.ratio(lead_city, icp_city) >= 80))
            geo = 1.0 if city_match else 0.5
        else:
            geo = 1.0
    else:
        geo = 0.5

    emp = _employee_overlap(_s(lead.get("employee_count")),
                            ctx.get("employee_count", "") or "")

    biz_text_parts: List[str] = []
    if ctx.get("company_stage"):
        biz_text_parts.append(_sl(ctx["company_stage"]))
    if ctx.get("product_service"):
        biz_text_parts.append(_sl(ctx["product_service"]))
    if ctx.get("target_seniority"):
        biz_text_parts.append(_sl(ctx["target_seniority"]))
    if ctx.get("prompt"):
        biz_text_parts.append(_sl(ctx["prompt"][:300]))
    icp_biz_text = " ".join(biz_text_parts)

    biz_score = 0.5
    lead_biz = _sl(lead.get("business", ""))
    if lead_biz and icp_biz_text:
        biz_score = _fuzz.partial_ratio(lead_biz, icp_biz_text) / 100.0
    elif not icp_biz_text:
        biz_score = 0.5

    desc_score = 0.5
    lead_desc = _sl(lead.get("description", ""))
    if lead_desc and icp_biz_text:
        desc_score = _fuzz.partial_ratio(lead_desc[:500], icp_biz_text) / 100.0
    elif not lead_desc:
        desc_score = 0.3

    role_type_dm = _role_type_dm_alignment(lead, ctx)
    dm_score = _decision_maker_score(lead, ctx)
    domain_overlap = _domain_keyword_overlap(lead, ctx.get("domain_words") or set())

    return {
        "geo": geo, "industry": ind, "sub_industry": sub,
        "role": role_score, "seniority": sen, "emp": emp,
        "business": biz_score, "description": desc_score,
        "role_type_dm": role_type_dm, "decision_maker": dm_score,
        "domain_overlap": domain_overlap,
    }

def _match_score(lead: Dict, ctx: Dict,
                 weights: Optional[Dict[str, float]] = None) -> float:
    feats = _extract_match_features(lead, ctx)
    w = weights or _DEFAULT_MATCH_WEIGHTS
    return (
        w.get("w_geo", 0.18) * feats["geo"]
        + w.get("w_industry", 0.17) * feats["industry"]
        + w.get("w_sub_industry", 0.16) * feats["sub_industry"]
        + w.get("w_role", 0.15) * feats["role"]
        + w.get("w_emp", 0.08) * feats["emp"]
        + w.get("w_business", 0.08) * feats.get("business", 0.5)
        + w.get("w_description", 0.08) * feats.get("description", 0.5)
        + w.get("w_seniority", 0.05) * feats.get("seniority", 0.5)
        + w.get("w_role_type_dm", 0.03) * feats.get("role_type_dm", 0.5)
        + w.get("w_decision_maker", 0.015) * feats.get("decision_maker", 0.5)
        + w.get("w_domain_overlap", 0.005) * feats.get("domain_overlap", 0.5)
    )

def _predict_icp_fit(feats: Dict[str, float]) -> float:
    return min(20.0,
        feats.get("industry", 0) * 4.0
        + feats.get("sub_industry", 0) * 3.0
        + feats.get("role", 0) * 4.0
        + feats.get("seniority", 0) * 3.0
        + feats.get("geo", 0) * 4.0
        + feats.get("emp", 0) * 2.0)

_DM_SENIORITY_BASE = {
    "C-Suite": 28.0, "VP": 25.0, "Director": 22.0,
    "Manager": 16.0, "Individual Contributor": 6.0,
}

def _predict_dm_score(lead: Dict, ctx: Dict) -> float:
    sen = _infer_seniority(_s(lead.get("role")), _s(lead.get("role_type")))
    base = _DM_SENIORITY_BASE.get(sen, 12.0)
    dm = _decision_maker_score(lead, ctx)
    return min(30.0, base * (0.7 + 0.3 * dm))

def _tokenize(text: str) -> Set[str]:
    return {w.lower() for w in re.findall(r'\w{2,}', text)}

def _build_icp_text(ctx: Dict) -> str:
    parts: List[str] = []
    if ctx.get("industry"):
        parts.append(f"Industry: {ctx['industry']}")
    if ctx.get("sub_industry"):
        parts.append(f"Sub-industry: {ctx['sub_industry']}")
    if ctx.get("target_roles"):
        parts.append(f"Roles: {', '.join(ctx['target_roles'])}")
    if ctx.get("target_seniority"):
        parts.append(f"Seniority: {ctx['target_seniority']}")
    if ctx.get("employee_count"):
        parts.append(f"Size: {ctx['employee_count']}")
    geo_parts: List[str] = []
    if ctx.get("city"):
        geo_parts.append(ctx["city"])
    if ctx.get("state"):
        geo_parts.append(ctx["state"])
    if ctx.get("country"):
        geo_parts.append(ctx["country"])
    if geo_parts:
        parts.append(f"Location: {', '.join(geo_parts)}")
    if ctx.get("product_service"):
        parts.append(f"Product: {ctx['product_service']}")
    if ctx.get("company_stage"):
        parts.append(f"Stage: {ctx['company_stage']}")
    if ctx.get("prompt"):
        parts.append(ctx["prompt"][:500])
    return ". ".join(parts)

def _build_lead_text(lead: Dict) -> str:
    parts = []
    for k in ("business", "industry", "sub_industry", "role"):
        v = _s(lead.get(k))
        if v:
            parts.append(v)
    desc = _s(lead.get("description"))
    if desc:
        parts.append(desc[:300])
    geo = []
    for k in ("city", "state", "country"):
        v = _s(lead.get(k))
        if v:
            geo.append(v)
    if geo:
        parts.append(", ".join(geo))
    return ". ".join(parts)

def _keyword_score(lead: Dict, ctx: Dict) -> float:
    icp_tokens = _tokenize(_build_icp_text(ctx))
    lead_tokens = _tokenize(_build_lead_text(lead))
    if not icp_tokens:
        return 0.5
    return min(1.0, len(icp_tokens & lead_tokens) / len(icp_tokens))

_SUB_INDUSTRY_THRESH = 70

def _filter_sub_industry(candidates: List[Dict], ctx: Dict) -> List[Dict]:
    icp_sub = _sl(ctx.get("sub_industry", ""))
    if not icp_sub:
        return candidates
    filtered = [c for c in candidates
                if _s(c.get("sub_industry"))
                and _fuzz.ratio(_sl(c.get("sub_industry", "")),
                                icp_sub) >= _SUB_INDUSTRY_THRESH]
    return filtered if filtered else candidates

_SUSPICIOUS_CHARS = re.compile(r'[<>{}|\\\^~`\[\]]')
_FAKE_COMPANY = [
    re.compile(r'^company\s*\d*$', re.I),
    re.compile(r'^corp(oration)?\s*\d*$', re.I),
    re.compile(r'^inc(orporated)?\s*\d*$', re.I),
    re.compile(r'^llc\s*\d*$', re.I),
    re.compile(r'^business\s*\d*$', re.I),
]
_FAKE_ROLE = [
    re.compile(r'^employee\s*$', re.I),
    re.compile(r'^worker\s*$', re.I),
    re.compile(r'^staff\s*$', re.I),
    re.compile(r'^person\s*$', re.I),
]

_PRECHECK_SENIORITY = ["C-Suite", "VP", "Director", "Manager", "Individual Contributor"]

def _precheck_candidate(lead: Dict, ctx: Dict) -> bool:
    biz = _s(lead.get("business"))
    role = _s(lead.get("role"))
    ind = _s(lead.get("industry"))
    sub = _s(lead.get("sub_industry"))
    if not biz or not role or not ind or not sub:
        return False
    linkedin = _s(lead.get("company_linkedin"))
    website = _s(lead.get("website") or lead.get("company_website"))
    if not linkedin and not website:
        return False
    for pat in _FAKE_COMPANY:
        if pat.match(biz):
            return False
    for pat in _FAKE_ROLE:
        if pat.match(role):
            return False
    for val in (biz, role):
        if _SUSPICIOUS_CHARS.search(val):
            return False
        if val.strip().isdigit():
            return False
        vl = val.lower()
        if vl in ("test", "asdf", "xxx", "sample", "example", "foo",
                   "bar", "null", "n/a", "none", "tbd"):
            return False
    if ctx["industry"] and _fuzz.ratio(_sl(ind), _sl(ctx["industry"])) < 80:
        return False
    if ctx["sub_industry"] and _fuzz.ratio(_sl(sub),
                                            _sl(ctx["sub_industry"])) < 70:
        return False
    if ctx["target_roles"]:
        best = max((_fuzz.partial_ratio(_sl(role), _sl(r))
                    for r in ctx["target_roles"] if r), default=0)
        if best < 60:
            return False
    icp_country = _s(ctx.get("country", ""))
    if icp_country:
        lead_country = _s(lead.get("country", ""))
        if not lead_country or lead_country.strip().lower() != icp_country.strip().lower():
            return False
    icp_sen = _s(ctx.get("target_seniority", ""))
    if icp_sen and icp_sen in _PRECHECK_SENIORITY:
        lead_sen = _infer_seniority(role, _s(lead.get("role_type", "")))
        if lead_sen in _PRECHECK_SENIORITY:
            lead_idx = _PRECHECK_SENIORITY.index(lead_sen)
            icp_idx = _PRECHECK_SENIORITY.index(icp_sen)
            if lead_idx > icp_idx + 1:
                return False
    return True

def _get_embeddings(texts: List[str], proxy_url: str
                    ) -> Optional[List[List[float]]]:
    if not proxy_url or not texts:
        return None
    endpoint = f"{proxy_url.rstrip('/')}/openrouter/embeddings"
    try:
        with httpx.Client(timeout=6.0) as c:
            r = c.post(endpoint, json={
                "model": "openai/text-embedding-3-small",
                "input": texts,
            })
            if r.status_code == 200:
                data = r.json()
                items = data.get("data", [])
                if len(items) == len(texts):
                    items.sort(key=lambda x: x.get("index", 0))
                    return [item["embedding"] for item in items]
    except Exception:
        pass
    return None

def _cosine_sim(a: List[float], b: List[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na < 1e-9 or nb < 1e-9:
        return 0.0
    return max(0.0, min(1.0, dot / (na * nb)))

def _hybrid_rerank_top2(candidates: List[Dict], ctx: Dict,
                        proxy_url: str,
                        match_weights: Optional[Dict[str, float]] = None
                        ) -> List[Tuple[Dict, float]]:
    """
    Hybrid RRF rerank → pick 5 best match candidates on 10 dimensions:
    industry, sub_industry, role, seniority, geo (strong), emp (medium),
    product, role_type_dm, decision_maker, domain_overlap. Hard gate:
    industry/sub-industry thresholds. Each candidate stores lead_id.
    """
    if not candidates:
        return []

    candidates = _filter_sub_industry(candidates, ctx)

    kw_scored: List[Tuple[Dict, float]] = []
    for c in candidates:
        match = _match_score(c, ctx, weights=match_weights)
        kw = _keyword_score(c, ctx)
        combined = 0.7 * match + 0.3 * kw
        kw_scored.append((c, combined))
    kw_scored.sort(key=lambda x: -x[1])

    icp_text = _build_icp_text(ctx)
    lead_texts = [_build_lead_text(lead) for lead, _ in kw_scored]
    all_texts = [icp_text] + lead_texts
    embeddings = _get_embeddings(all_texts, proxy_url) if proxy_url else None

    rrf_k = 50
    if embeddings and len(embeddings) == len(all_texts):
        icp_emb = embeddings[0]
        sem_scored = [
            (lead, _cosine_sim(icp_emb, embeddings[i + 1]))
            for i, (lead, _) in enumerate(kw_scored)
        ]
        sem_scored.sort(key=lambda x: -x[1])

        kw_rank = {str(lead.get("id", id(lead))): rank
                   for rank, (lead, _) in enumerate(kw_scored, 1)}
        sem_rank = {str(lead.get("id", id(lead))): rank
                    for rank, (lead, _) in enumerate(sem_scored, 1)}

        rrf_scores: List[Tuple[Dict, float]] = []
        seen: Set[str] = set()
        for lead, _ in kw_scored:
            kid = str(lead.get("id", id(lead)))
            if kid in seen:
                continue
            seen.add(kid)
            r1 = kw_rank.get(kid, 999)
            r2 = sem_rank.get(kid, 999)
            rrf = 1.0 / (rrf_k + r1) + 1.0 / (rrf_k + r2)
            rrf_scores.append((lead, rrf))
        rrf_scores.sort(key=lambda x: -x[1])
        ranked = rrf_scores
    else:
        ranked = kw_scored

    top2: List[Tuple[Dict, float]] = []
    for lead, score in ranked:
        if _precheck_candidate(lead, ctx):
            lid = lead.get("id") or lead.get("lead_id")
            if lid is not None:
                lead["lead_id"] = int(lid)
            top2.append((lead, score))
            if len(top2) >= 10:
                break

    if not top2:
        icp_ind = _sl(ctx.get("industry", ""))
        for lead, score in ranked[:30]:
            r_ind = _sl(lead.get("industry", ""))
            if icp_ind and r_ind and _fuzz.ratio(r_ind, icp_ind) < 80:
                continue
            lid = lead.get("id") or lead.get("lead_id")
            if lid is not None:
                lead["lead_id"] = int(lid)
            top2.append((lead, score))
            if len(top2) >= 10:
                break
        if not top2:
            for lead, score in ranked[:10]:
                lid = lead.get("id") or lead.get("lead_id")
                if lid is not None:
                    lead["lead_id"] = int(lid)
                top2.append((lead, score))

    return top2

def _llm_rerank_candidates(
    candidates: List[Tuple[Dict, float]],
    ctx: Dict,
    proxy_url: str,
) -> List[Tuple[Dict, float]]:
    if not candidates or not proxy_url:
        return candidates[:5]

    pool = candidates[:20]
    icp_text = _build_icp_text(ctx)

    candidate_lines: List[str] = []
    for i, (lead, _score) in enumerate(pool):
        line = (
            f"#{i+1}: {_s(lead.get('business'))} | "
            f"Industry: {_s(lead.get('industry'))} | "
            f"Sub-industry: {_s(lead.get('sub_industry'))} | "
            f"Role: {_s(lead.get('role'))} ({_s(lead.get('role_type'))}) | "
            f"Country: {_s(lead.get('country'))} | "
            f"State: {_s(lead.get('state'))} | "
            f"City: {_s(lead.get('city'))} | "
            f"Employees: {_s(lead.get('employee_count'))} | "
            f"Desc: {_s(lead.get('description', ''))[:120]}"
        )
        candidate_lines.append(line)

    prompt = (
        "You are a B2B lead qualification expert. Rank these candidates "
        "by how well they match the Ideal Customer Profile (ICP).\n\n"
        f"ICP:\n{icp_text}\n\n"
        f"CANDIDATES:\n" + "\n".join(candidate_lines) + "\n\n"
        "Select the TOP 10 candidates that BEST match this ICP. Priority order:\n"
        "1. Geographic match (country, state, city) — HIGHEST priority\n"
        "2. Industry alignment\n"
        "3. Sub-industry alignment\n"
        "4. Role match with target roles\n"
        "5. Company size (employee count) match\n"
        "6. Business relevance to ICP product/service and company stage\n"
        "7. Description match to ICP context\n\n"
        "Return ONLY a JSON array of the candidate numbers ordered from "
        "best to worst match, e.g. [3, 1, 7, ...]\n"
        "Respond with ONLY the JSON array, no markdown."
    )

    resp = _openrouter_chat(prompt, proxy_url)
    if not resp:
        return pool[:10]

    try:
        cleaned = resp.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)
        ranked_indices = json.loads(cleaned)
        if not isinstance(ranked_indices, list):
            return pool[:10]

        result: List[Tuple[Dict, float]] = []
        seen: Set[int] = set()
        for idx in ranked_indices:
            i = int(idx) - 1
            if 0 <= i < len(pool) and i not in seen:
                result.append(pool[i])
                seen.add(i)
            if len(result) >= 5:
                break
        for i, item in enumerate(pool):
            if i not in seen and len(result) < 5:
                result.append(item)
                seen.add(i)
        return result
    except Exception:
        return pool[:10]

def _validate_date_obj(y: int, m: int, d: int) -> Optional[date]:
    try:
        dt = date(y, m, d)
    except ValueError:
        return None
    today = date.today()
    if dt > today or (today - dt).days > 730:
        return None
    return dt

def _parse_iso_to_date(s: str) -> Optional[date]:
    m = re.match(r'(20\d{2})-(\d{2})-(\d{2})', s.strip())
    if m:
        return _validate_date_obj(int(m.group(1)), int(m.group(2)),
                                  int(m.group(3)))
    return None


def _parse_relative_date_from_api(relative_str: str) -> Optional[str]:
    s = relative_str.strip().lower()
    s = re.sub(r'\s+edited\s*$', '', s).strip()

    ref = date.today()
    age_days = 0

    m_ago = re.match(r'^(\d+)\s*days?\s*ago$', s)
    if m_ago:
        age_days = int(m_ago.group(1))
        return (ref - timedelta(days=age_days)).isoformat()

    s_compact = re.sub(r'\s+', '', s)
    m = re.match(r'^(\d+)h$', s_compact)
    if m:
        return ref.isoformat()
    m = re.match(r'^(\d+)d$', s_compact)
    if m:
        age_days = int(m.group(1))
        return (ref - timedelta(days=age_days)).isoformat()
    m = re.match(r'^(\d+)w$', s_compact)
    if m:
        age_days = int(m.group(1)) * 7
        return (ref - timedelta(days=age_days)).isoformat()
    m = re.match(r'^(\d+)mo$', s_compact)
    if m:
        months = int(m.group(1))
        target = ref
        for _ in range(months):
            target = (target.replace(day=1) - timedelta(days=1))
        return target.isoformat()
    m = re.match(r'^(\d+)yr$', s_compact)
    if m:
        years = int(m.group(1))
        try:
            return ref.replace(year=ref.year - years).isoformat()
        except ValueError:
            return ref.replace(year=ref.year - years, day=28).isoformat()
    return None


def _collect_dates_html(html: str) -> List[date]:
    dates: List[date] = []
    if not html:
        return dates
    chunk = html[:25000]
    for m in re.finditer(
        r'"(?:datePosted|datePublished|dateModified|dateCreated)"'
        r'\s*:\s*"(20\d{2}-\d{2}-\d{2}[^"]*)"', chunk, re.I
    ):
        dt = _parse_iso_to_date(m.group(1))
        if dt:
            dates.append(dt)
    for m in re.finditer(
        r'content\s*=\s*["\'](\d{4}-\d{2}-\d{2})["\']', chunk[:15000]
    ):
        dt = _parse_iso_to_date(m.group(1))
        if dt:
            dates.append(dt)
    for m in re.finditer(
        r'<time[^>]*datetime\s*=\s*["\']([^"\']+)["\']',
        chunk[:15000], re.I
    ):
        dt = _parse_iso_to_date(m.group(1))
        if dt:
            dates.append(dt)
    for m in re.finditer(
        r'property\s*=\s*["\']article:(?:published|modified)_time["\']'
        r'\s+content\s*=\s*["\']([^"\']+)["\']', chunk[:15000], re.I
    ):
        dt = _parse_iso_to_date(m.group(1))
        if dt:
            dates.append(dt)
    for m in re.finditer(
        r'name\s*=\s*["\'](?:date|pubdate|publish[_-]?date|last[_-]?modified)["\']'
        r'\s+content\s*=\s*["\']([^"\']+)["\']', chunk[:15000], re.I
    ):
        dt = _parse_iso_to_date(m.group(1))
        if dt:
            dates.append(dt)
    for m in re.finditer(
        r'data-(?:date|timestamp|time|published)\s*=\s*["\']([^"\']+)["\']',
        chunk[:15000], re.I
    ):
        val = m.group(1).strip()
        dt = _parse_iso_to_date(val[:10])
        if dt:
            dates.append(dt)
        elif val.isdigit() and len(val) >= 10:
            try:
                ts = int(val[:10])
                if 1_400_000_000 <= ts <= 2_000_000_000:
                    from datetime import datetime as _dt
                    d = _dt.utcfromtimestamp(ts).date()
                    dv = _validate_date_obj(d.year, d.month, d.day)
                    if dv:
                        dates.append(dv)
            except (ValueError, OSError):
                pass
    for m in re.finditer(
        r'"(?:updated_at|created_at|published_at|modified_at|timestamp|post_date)"'
        r'\s*:\s*"(20\d{2}-\d{2}-\d{2}[^"]*)"', chunk[:20000], re.I
    ):
        dt = _parse_iso_to_date(m.group(1))
        if dt:
            dates.append(dt)
    for m in re.finditer(
        r'"(?:updated_at|created_at|published_at|timestamp|date|time)"'
        r'\s*:\s*(\d{10})\b', chunk[:20000]
    ):
        try:
            ts = int(m.group(1))
            if 1_400_000_000 <= ts <= 2_000_000_000:
                from datetime import datetime as _dt
                d = _dt.utcfromtimestamp(ts).date()
                dv = _validate_date_obj(d.year, d.month, d.day)
                if dv:
                    dates.append(dv)
        except (ValueError, OSError):
            pass
    for m in re.finditer(
        r'class\s*=\s*["\'](?:entry-date|posted-on|date-published|publish-date)["\']'
        r'[^>]*>([^<]{6,30})<', chunk[:15000], re.I
    ):
        inner = m.group(1).strip()
        dt = _parse_iso_to_date(inner[:10])
        if dt:
            dates.append(dt)
    return dates

_COPYRIGHT_RX = re.compile(
    r'(?:©|\(c\)|copyright)\s*(?:19|20)\d{2}(?:\s*[-–]\s*(?:19|20)\d{2})?',
    re.I,
)
_FOUNDED_RX = re.compile(
    r'(?:founded|established|since|est\.?)\s+(?:in\s+)?(?:19|20)\d{2}',
    re.I,
)
_BOILERPLATE_DATE_RX = re.compile(
    r'(?:(?:last\s+)?(?:updated|modified|refreshed|generated|retrieved|accessed|fetched)'
    r'|as\s+of|current\s+(?:as\s+of|date)|page\s+(?:updated|generated|modified)'
    r'|date\s*:)\s*[:\-]?\s*'
    r'(?:\w+\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4})',
    re.I,
)

def _strip_non_intent_dates(text: str) -> str:
    text = _COPYRIGHT_RX.sub('', text)
    text = _FOUNDED_RX.sub('', text)
    text = _BOILERPLATE_DATE_RX.sub('', text)
    return text

def _collect_dates_text(text: str) -> List[date]:
    dates: List[date] = []
    if not text:
        return dates
    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    for m in re.finditer(
        r'(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|'
        r'Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|'
        r'Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
        r'\s+(\d{1,2}),?\s+(20\d{2})', text, re.I
    ):
        mo = month_map.get(m.group(1)[:3].lower())
        if mo:
            dt = _validate_date_obj(int(m.group(3)), mo, int(m.group(2)))
            if dt:
                dates.append(dt)
    for m in re.finditer(r'(20\d{2})-(\d{2})-(\d{2})', text):
        dt = _validate_date_obj(int(m.group(1)), int(m.group(2)),
                                int(m.group(3)))
        if dt:
            dates.append(dt)
    for m in re.finditer(
        r'(\d{1,2})\s+'
        r'(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|'
        r'Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|'
        r'Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
        r',?\s+(20\d{2})', text, re.I
    ):
        mo = month_map.get(m.group(2)[:3].lower())
        if mo:
            dt = _validate_date_obj(int(m.group(3)), mo, int(m.group(1)))
            if dt:
                dates.append(dt)
    for m in re.finditer(
        r'(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|'
        r'Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|'
        r'Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
        r',?\s+(20\d{2})\b', text, re.I
    ):
        mo = month_map.get(m.group(1)[:3].lower())
        yr = int(m.group(2))
        if mo and 2015 <= yr <= date.today().year:
            dt = _validate_date_obj(yr, mo, 15)
            if dt:
                dates.append(dt)
    _quarter_month = {"1": 2, "2": 5, "3": 8, "4": 11}
    for m in re.finditer(r'\bQ([1-4])\s+(20\d{2})\b', text[:3000], re.I):
        qm = _quarter_month.get(m.group(1))
        yr = int(m.group(2))
        if qm and 2015 <= yr <= date.today().year:
            dt = _validate_date_obj(yr, qm, 15)
            if dt:
                dates.append(dt)
    for m in re.finditer(r'\bH([12])\s+(20\d{2})\b', text[:3000], re.I):
        hm = 3 if m.group(1) == "1" else 9
        yr = int(m.group(2))
        if 2015 <= yr <= date.today().year:
            dt = _validate_date_obj(yr, hm, 15)
            if dt:
                dates.append(dt)
    _season_month = {"spring": 4, "summer": 7, "fall": 10, "autumn": 10, "winter": 1}
    for m in re.finditer(
        r'\b(spring|summer|fall|autumn|winter)\s+(20\d{2})\b', text[:3000], re.I
    ):
        sm = _season_month.get(m.group(1).lower())
        yr = int(m.group(2))
        if sm and 2015 <= yr <= date.today().year:
            dt = _validate_date_obj(yr, sm, 15)
            if dt:
                dates.append(dt)
    for m in re.finditer(r'\b(?:FY\s?|fiscal\s+year\s+)(20\d{2})\b', text[:3000], re.I):
        yr = int(m.group(1))
        if 2015 <= yr <= date.today().year:
            dt = _validate_date_obj(yr, 6, 15)
            if dt:
                dates.append(dt)
    return dates

def _extract_best_date(html: Optional[str], text: str,
                       url: str, strict: bool = True) -> Optional[str]:
    all_dates: List[date] = []
    if html:
        stripped_html = _strip_non_intent_dates(html[:8000])
        all_dates.extend(_collect_dates_html(stripped_html))
        all_dates.extend(_collect_dates_text(stripped_html))
    stripped_text = _strip_non_intent_dates(text)
    all_dates.extend(_collect_dates_text(stripped_text))
    if not all_dates:
        return None
    newest = max(set(all_dates))
    if strict and (date.today() - newest).days > 365:
        return None
    return newest.isoformat()

def _classify_source(url: str) -> str:
    ul = url.lower()
    domain = urlparse(ul).netloc
    if "linkedin.com" in domain:
        return "linkedin"
    if any(d in domain for d in JOB_BOARD_DOMAINS):
        return "job_board"
    if "github.com" in domain:
        return "github"
    if "twitter.com" in domain or "x.com" in domain:
        return "social_media"
    if "wikipedia.org" in domain:
        return "wikipedia"
    if any(d in domain for d in (
        "g2.com", "trustradius.com", "capterra.com",
        "gartner.com",
    )):
        return "review_site"
    if any(d in domain for d in (
        "techcrunch.com", "bloomberg.com", "reuters.com",
        "businesswire.com", "prnewswire.com", "globenewswire.com",
        "venturebeat.com", "forbes.com", "crunchbase.com",
    )):
        return "news"
    return "company_website"

def _trim_sentences(text: str, max_chars: int = 480) -> str:
    if len(text) <= max_chars:
        return text
    trunc = text[:max_chars]
    last = max(trunc.rfind(". "), trunc.rfind("! "), trunc.rfind("? "))
    if last > 80:
        return trunc[:last + 1]
    return trunc.rstrip() + "."

_ICP_STOPWORDS = frozenset({
    "the", "a", "an", "and", "or", "for", "to", "in", "on", "at", "of",
    "is", "are", "that", "this", "with", "by", "from", "as", "it",
})

def _extract_icp_keywords(intent_signals: Optional[List[str]]) -> Optional[Set[str]]:
    if not intent_signals:
        return None
    kw: Set[str] = set()
    for sig in intent_signals:
        if not isinstance(sig, str):
            continue
        sig = _s(sig).lower().strip()
        if len(sig) < 3:
            continue
        words = [w for w in re.split(r"[\s,]+", sig) if len(w) >= 2 and w not in _ICP_STOPWORDS]
        kw.update(words)
        if len(words) >= 2:
            kw.add(" ".join(words[:3]))
    return kw if kw else None

_llm_icp_kw_cache: Dict[str, Optional[Set[str]]] = {}

def _extract_icp_keywords_llm(ctx: Dict, proxy_url: str) -> Optional[Set[str]]:
    prompt_text = _s(ctx.get("prompt", ""))
    intent_signals = ctx.get("intent_signals") or []
    product_service = _s(ctx.get("product_service", ""))
    industry = _s(ctx.get("industry", ""))
    sub_industry = _s(ctx.get("sub_industry", ""))

    cache_key = f"{prompt_text[:200]}|{product_service}|{'|'.join(str(s) for s in intent_signals[:3])}"
    if cache_key in _llm_icp_kw_cache:
        return _llm_icp_kw_cache[cache_key]

    fallback = _extract_icp_keywords(intent_signals)

    if not proxy_url or (not prompt_text and not intent_signals and not product_service):
        _llm_icp_kw_cache[cache_key] = fallback
        return fallback

    sig_str = ", ".join(str(s) for s in intent_signals[:5]) if intent_signals else "none"
    llm_prompt = f"""Extract search keywords from this B2B lead qualification request.

ICP PROMPT: {prompt_text[:500]}
INTENT SIGNALS: {sig_str}
PRODUCT/SERVICE: {product_service or 'not specified'}
INDUSTRY: {industry or 'not specified'}
SUB-INDUSTRY: {sub_industry or 'not specified'}

Return 15-25 specific, lowercase keywords/phrases a salesperson would search for to find evidence of buying intent on a company website or news article. Include:
1. Action keywords from the intent signals (e.g., hiring, recruiting, funding, launch, expansion, partnership, acquisition, migration)
2. Product/technology keywords related to the product/service (e.g., specific tools, platforms, technologies)
3. Industry-specific terms that indicate buying signals
4. Job title keywords if hiring is mentioned
5. Financial keywords if funding/growth is mentioned

Respond with ONLY a JSON array of strings, no markdown:
["keyword1", "keyword2", "two word phrase", ...]"""

    try:
        resp = _openrouter_chat(llm_prompt, proxy_url)
        if resp:
            resp = resp.strip()
            if resp.startswith("```"):
                resp = re.sub(r'^```(?:json)?\s*', '', resp)
                resp = re.sub(r'\s*```$', '', resp)
            parsed = json.loads(resp)
            if isinstance(parsed, list) and len(parsed) >= 3:
                kw: Set[str] = set()
                for item in parsed:
                    if isinstance(item, str):
                        w = item.strip().lower()
                        if 2 <= len(w) <= 50 and w not in _ICP_STOPWORDS:
                            kw.add(w)
                if fallback:
                    kw.update(fallback)
                if kw:
                    _llm_icp_kw_cache[cache_key] = kw
                    return kw
    except Exception:
        pass

    _llm_icp_kw_cache[cache_key] = fallback
    return fallback

def _score_sentence(s: str, company: str,
                    icp_keywords: Optional[Set[str]] = None) -> float:
    sl = s.lower()
    sc = 0.0
    for anchor in EVIDENCE_ANCHORS:
        if anchor in sl:
            sc += 4.0
    if company.lower() in sl:
        sc += 3.0
    if re.search(r'\$[\d,.]+|[\d,.]+ (?:million|billion)', sl):
        sc += 3.0
    elif re.search(r'\d', s):
        sc += 1.0
    if icp_keywords:
        for kw in icp_keywords:
            if len(kw) >= 4 and kw in sl:
                sc += 5.0
                break
    return sc

def _build_evidence_text(text: str, company: str,
                         max_chars: int = 480,
                         icp_keywords: Optional[Set[str]] = None) -> str:
    sentences = re.split(r'(?<=[.!?])\s+', text)
    scored: List[Tuple[float, str]] = []
    for s in sentences:
        if len(s) < 25 or len(s) > 500:
            continue
        sl = s.lower()
        if any(b in sl for b in ("cookie", "privacy", "terms of",
                                  "copyright", "subscribe", "sign up")):
            continue
        scored.append((_score_sentence(s, company, icp_keywords), s))
    if not scored:
        return _trim_sentences(text[:max_chars], max_chars)
    scored.sort(key=lambda x: x[0], reverse=True)
    parts: List[str] = []
    total = 0
    for sc, s in scored:
        if total + len(s) > max_chars:
            break
        parts.append(s)
        total += len(s) + 1
        if len(parts) >= 3:
            break
    return " ".join(parts) if parts else scored[0][1][:max_chars]

def _extract_evidence_item(url: str, html: Optional[str],
                           company: str,
                           source_type: str,
                           lenient: bool = False,
                           icp_keywords: Optional[Set[str]] = None) -> Optional[Dict]:
    if not html or len(html) < (120 if lenient else 300):
        return None
    text = _html_to_text(html)[:8000]
    if len(text) < (50 if lenient else 100):
        return None
    tl = text.lower()
    if any(s in tl for s in ("please log in", "sign in to continue",
                              "access denied", "404 not found",
                              "page not found")):
        return None
    if _is_weak_page(url, tl):
        return None
    sig_date = _extract_best_date(html, text, url, strict=(not lenient))
    if not sig_date and lenient:
        sig_date = _extract_best_date(html, text, url, strict=False)
    if not sig_date and not lenient:
        return None
    extracted = _build_evidence_text(text, company, 480, icp_keywords)
    if not extracted or len(extracted) < 40:
        return None
    return {
        "url": url,
        "source": source_type,
        "extracted_date": sig_date or "",
        "extracted_text": _trim_sentences(extracted, 480),
    }

def _fetch_google_news_sd(query: str, proxy_url: str,
                          country: str = "us",
                          timeout: float = 4.0) -> List[Dict]:
    if not proxy_url or not query:
        return []
    ep = (f"{proxy_url.rstrip('/')}/scrapingdog/google_news"
          f"?query={quote(query)}&country={quote(country)}"
          f"&domain=google.com")
    try:
        resp = _proxy_get_json(ep, timeout=timeout)
        if resp and isinstance(resp, dict):
            results = resp.get("news_results") or []
            return [r for r in results if isinstance(r, dict)]
    except Exception:
        pass
    return []


def _llm_extract_intent_from_data(
    raw_data: str,
    source_type: str,
    company: str,
    icp_prompt: str,
    icp_intent_signals: Optional[List[str]],
    proxy_url: str,
    icp_ctx: Optional[Dict] = None,
) -> Optional[Dict[str, str]]:
    if not raw_data or not proxy_url or len(raw_data) < 50:
        return None

    sig_str = ", ".join(str(s) for s in (icp_intent_signals or [])[:5])

    icp_details = ""
    if icp_ctx:
        prod = _s(icp_ctx.get("product_service", ""))
        stage = _s(icp_ctx.get("company_stage", ""))
        roles = ", ".join(_s(r) for r in (icp_ctx.get("target_roles") or [])[:3] if _s(r))
        sen = _s(icp_ctx.get("target_seniority", ""))
        ind = _s(icp_ctx.get("industry", ""))
        sub = _s(icp_ctx.get("sub_industry", ""))
        parts = []
        if prod:
            parts.append(f"BUYER IS SELLING: {prod}")
        if ind:
            parts.append(f"TARGET INDUSTRY: {ind}" + (f" / {sub}" if sub else ""))
        if roles:
            parts.append(f"TARGET ROLES: {roles}")
        if sen:
            parts.append(f"TARGET SENIORITY: {sen}")
        if stage:
            parts.append(f"COMPANY STAGE: {stage}")
        if parts:
            icp_details = "\n".join(parts) + "\n"

    prompt = (
        "You are a B2B lead qualification expert. Extract the STRONGEST "
        "intent signal from this company data that matches the buyer's ICP.\n\n"
        f"COMPANY: {company}\n"
        f"{icp_details}"
        f"ICP INTENT SIGNALS TO MATCH: {sig_str or 'hiring, funding, expansion, product launch, partnership'}\n"
        f"BUYER REQUEST: {icp_prompt[:400]}\n"
        f"SOURCE TYPE: {source_type}\n\n"
        f"RAW DATA (truncated):\n{raw_data[:3000]}\n\n"
        "TASK: Find the text that BEST proves this company is a good lead "
        "for the buyer. A salesperson selling the buyer's product would use "
        "this text to justify reaching out.\n\n"
        "PRIORITY ORDER (best to worst):\n"
        "1. Specific actions matching ICP intent: hiring the target roles, "
        "funding rounds, product launches, expansions, partnerships\n"
        "2. Business activity showing buyer-relevant needs: new products, "
        "customer wins, technology adoption, certifications\n"
        "3. Company capabilities relevant to what the buyer sells\n\n"
        "CRITICAL RULES:\n"
        "- description: Copy VERBATIM text from the data (exact words). "
        "Must be 50+ chars. Do NOT paraphrase or summarize.\n"
        "- REJECT pure taglines, navigation text, or cookie notices. "
        "The text MUST show what the company DOES or has DONE.\n"
        "- date: Extract ONLY dates that appear in the data. "
        "If a post says '6h ago' or '1d' or '1w', leave date EMPTY. "
        "Only use YYYY-MM-DD if an actual date is in the text.\n"
        "- If the data only contains navigation, login forms, or cookie "
        "banners with no substantive content, return null.\n\n"
        "Return ONLY JSON (no markdown):\n"
        '{"description": "verbatim text from data", '
        '"date": "YYYY-MM-DD or empty string if no exact date", '
        '"found": true} or null if nothing substantive found'
    )
    resp = _openrouter_chat(prompt, proxy_url)
    if not resp:
        return None
    try:
        cleaned = resp.strip()
        if cleaned.lower() == "null":
            return None
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)
        parsed = json.loads(cleaned)
        if not parsed or not isinstance(parsed, dict):
            return None
        if not parsed.get("found"):
            return None
        desc = _s(parsed.get("description", ""))
        if len(desc) < 30:
            return None
        claimed_date = _s(parsed.get("date", ""))
        if claimed_date:
            dates_in_text = _collect_dates_text(raw_data[:4000])
            date_strs = {d.isoformat() for d in dates_in_text}
            if claimed_date not in date_strs:
                year_month = claimed_date[:7]
                if not any(ds.startswith(year_month) for ds in date_strs):
                    claimed_date = ""
        return {
            "description": desc[:499],
            "date": claimed_date,
        }
    except Exception:
        return None


_llm_query_cache: Dict[str, List[str]] = {}

def _llm_build_queries(ctx: Dict, company: str, proxy_url: str) -> List[str]:
    sigs = ctx.get("intent_signals") or []
    prod = _s(ctx.get("product_service", ""))
    sub = _s(ctx.get("sub_industry", ""))
    ck = f"sq|{prod}|{sub}|{'|'.join(str(s)[:20] for s in sigs[:3])}"
    if ck in _llm_query_cache:
        return _llm_query_cache[ck]
    terms: List[str] = []
    for s in sigs[:3]:
        terms.extend(w for w in str(s).lower().split() if len(w) > 2)
    if prod:
        terms.extend(w for w in prod.lower().split() if len(w) > 2)
    if sub:
        terms.extend(w for w in sub.lower().split() if len(w) > 2)
    terms = list(dict.fromkeys(terms))[:8]
    if not proxy_url or not terms:
        _llm_query_cache[ck] = terms
        return terms
    p = (f"Generate 4 short search queries (3-6 words each) to find evidence of "
         f"intent signals for a company.\n"
         f"ICP: {_s(ctx.get('prompt',''))[:250]}\n"
         f"Signals: {', '.join(str(s) for s in sigs[:3])}\n"
         f"Product: {prod}\nSub-industry: {sub}\n"
         f"Return ONLY a JSON array of 4 strings, no markdown.")
    r = _openrouter_chat(p, proxy_url)
    if r:
        try:
            c = r.strip()
            if c.startswith("```"):
                c = re.sub(r'^```(?:json)?\s*', '', c)
                c = re.sub(r'\s*```$', '', c)
            parsed = json.loads(c)
            if isinstance(parsed, list):
                terms = [str(q).strip() for q in parsed[:4]
                         if isinstance(q, str) and len(str(q).strip()) > 3]
        except Exception:
            pass
    _llm_query_cache[ck] = terms
    return terms

_SOURCES_DATE_REQUIRED = frozenset({"linkedin", "job_board", "news", "social_media"})
_SOURCES_DATE_NOT_REQUIRED = frozenset({"github", "company_website", "wikipedia", "review_site"})
_MAX_INTENT_NO_DATE_REQUIRED = 15

_VALIDATOR_GENERIC_RX = [
    re.compile(r'is\s+actively\s+operating\s+in\s+\w+', re.I),
    re.compile(r'visible\s+market\s+activity', re.I),
    re.compile(r'market\s+activity\s+and\s+company\s+updates', re.I),
    re.compile(r'business\s+operations\s+and\s+updates', re.I),
    re.compile(r'^.{0,50}\s+is\s+(?:actively\s+)?(?:operating|expanding|growing)', re.I),
    re.compile(r'company\s+(?:updates|activities|operations)', re.I),
    re.compile(r'market\s+(?:activity|presence|operations)', re.I),
]
_VALIDATOR_TEMPLATE_RX = re.compile(
    r'^\w+(?:\s+\w+){0,3}\s+is\s+\w+ing\s+(?:in\s+)?\w+\s*\.?$', re.I)

def _is_generic_description(desc: str) -> bool:
    dl = desc.strip().lower()
    for rx in _VALIDATOR_GENERIC_RX:
        if rx.search(dl):
            return True
    has_specific = any(kw in dl for kw in SPECIFIC_INTENT_KW)
    if len(dl) < 80 and not has_specific:
        return True
    if _VALIDATOR_TEMPLATE_RX.match(dl) and not has_specific:
        return True
    return False

_MONTH_NAMES_MAP = {
    1: ("january", "jan"), 2: ("february", "feb"), 3: ("march", "mar"),
    4: ("april", "apr"), 5: ("may", "may"), 6: ("june", "jun"),
    7: ("july", "jul"), 8: ("august", "aug"), 9: ("september", "sep"),
    10: ("october", "oct"), 11: ("november", "nov"), 12: ("december", "dec"),
}

def _check_date_in_content(claimed_date: str, content: str) -> str:
    try:
        dt = datetime.strptime(claimed_date.strip()[:10], "%Y-%m-%d")
    except (ValueError, AttributeError):
        return "no_match"
    year, month, day = dt.year, dt.month, dt.day
    year_str = str(year)
    month_names = _MONTH_NAMES_MAP.get(month, ())
    cl = content.lower()
    iso_date = f"{year:04d}-{month:02d}-{day:02d}"
    if iso_date in content:
        return "verified"
    for full_name, abbrev in [month_names] if month_names else []:
        ds, dp = str(day), f"{day:02d}"
        for m in (full_name, abbrev):
            if re.search(rf'\b{m}\s+{ds}\b[,]?\s*{year_str}', cl):
                return "verified"
            if dp != ds and re.search(rf'\b{m}\s+{dp}\b[,]?\s*{year_str}', cl):
                return "verified"
            if re.search(rf'\b{ds}\s+{m}\b[,]?\s*{year_str}', cl):
                return "verified"
    if re.search(rf'date\w*["\']?\s*[:=]\s*["\']?{re.escape(iso_date)}', cl):
        return "verified"
    month_year_found = False
    for m in month_names:
        if re.search(rf'\b{m}\s+{year_str}\b', cl):
            month_year_found = True
            break
    iso_month = f"{year:04d}-{month:02d}"
    if iso_month in content:
        month_year_found = True
    if month_year_found:
        return "approximate" if day == 1 else "verified"
    if re.search(rf'\b{year_str}\b', cl):
        return "year_only"
    return "no_match"

def _score_evidence_item(ev: Dict, company: str,
                         icp_kw: Optional[Set[str]] = None,
                         raw_content: str = "") -> float:
    txt = (ev.get("extracted_text") or "").lower()
    if not txt:
        return 0.0
    src = ev.get("source", "")
    d = ev.get("extracted_date", "")

    if _is_generic_description(txt):
        ev["_date_status"] = "fabricated"
        return 0.0

    if src == "other" and len(txt) < 100:
        ev["_date_status"] = "fabricated"
        return 0.0

    date_status = "no_date"
    if d:
        content_for_check = _strip_non_intent_dates(raw_content or txt)
        precision = _check_date_in_content(d, content_for_check)
        if precision in ("verified", "approximate"):
            date_status = "verified"
        elif precision == "year_only":
            date_status = "no_date"
        elif precision == "no_match":
            date_status = "fabricated"
    ev["_date_status"] = date_status

    if date_status == "fabricated":
        return 0.0

    relevance = 0.0
    icp_hits = sum(1 for kw in (icp_kw or set()) if kw in txt)
    relevance += min(15.0, icp_hits * 5.0)
    intent_hits = sum(1 for skw in SPECIFIC_INTENT_KW if skw in txt)
    relevance += min(15.0, intent_hits * 4.0)
    anchor_hits = sum(1 for a in EVIDENCE_ANCHORS if a in txt)
    relevance += min(10.0, anchor_hits * 3.0)
    if company and company.lower() in txt:
        relevance += 5.0
    if re.search(r'\$[\d,.]+|[\d,.]+ (?:million|billion|mn|bn)', txt):
        relevance += 5.0
    if len(txt) >= 200:
        relevance += 2.0
    elif len(txt) >= 100:
        relevance += 1.0
    relevance = min(50.0, relevance)

    if date_status == "no_date" and src in _SOURCES_DATE_REQUIRED:
        relevance = min(relevance, _MAX_INTENT_NO_DATE_REQUIRED)

    confidence = 80
    if date_status == "verified":
        confidence = 85
    elif date_status == "no_date":
        if src in _SOURCES_DATE_NOT_REQUIRED:
            confidence = 75
        else:
            confidence = 50

    source_mult = SOURCE_MULTIPLIERS.get(src, 0.3)

    time_decay = 1.0
    if d:
        try:
            age = (date.today() - date.fromisoformat(d)).days
            if age <= 60:
                time_decay = 1.0
            elif age <= 365:
                time_decay = 0.5
            else:
                time_decay = 0.25
        except Exception:
            time_decay = 0.5
    elif src in _SOURCES_DATE_REQUIRED:
        time_decay = 0.5

    return relevance * (confidence / 100.0) * source_mult * time_decay

def _discover_intent_sources(lead: Dict, company: str,
                             proxy_url: str, t0: float,
                             source_priority: Optional[List[str]] = None,
                             icp_intent_signals: Optional[List[str]] = None,
                             icp_kw: Optional[Set[str]] = None,
                             icp_text: str = "",
                             icp_target_roles: Optional[List[str]] = None,
                             icp_ctx: Optional[Dict] = None,
                             ) -> List[Dict]:
    evidence: List[Dict] = []
    found_sources: Set[str] = set()
    if icp_kw is None:
        icp_kw = _extract_icp_keywords(icp_intent_signals)
    _target_roles: List[str] = [_s(r) for r in (icp_target_roles or []) if _s(r)]
    _icp_queries: List[str] = []
    if icp_ctx and proxy_url and (_PIPELINE_BUDGET - (time.time() - t0)) > 8.0:
        _icp_queries = _llm_build_queries(icp_ctx, company, proxy_url)

    def _tl():
        return _PIPELINE_BUDGET - (time.time() - t0)

    def _probe_linkedin():
        linkedin_url = _s(lead.get("company_linkedin"))
        if not linkedin_url and company:
            slug = re.sub(r'[^a-z0-9]+', '-', company.lower()).strip('-')
            if slug:
                linkedin_url = f"https://www.linkedin.com/company/{slug}"
        if not linkedin_url or _tl() < 2.0:
            return
        if not linkedin_url.startswith("http"):
            linkedin_url = "https://" + linkedin_url
        raw = _fetch_linkedin_company(linkedin_url, proxy_url)
        if not raw or len(raw) < 100:
            return
        data: Optional[Dict] = None
        if raw.strip().startswith(("[", "{")):
            try:
                parsed_json = json.loads(raw)
                data = parsed_json[0] if isinstance(parsed_json, list) and parsed_json else parsed_json
            except json.JSONDecodeError:
                pass

        if not isinstance(data, dict):
            text_clean = _html_to_text(raw) if "<" in raw else raw
            sig_date = _extract_best_date(
                raw if "<" in raw else None, text_clean[:6000], linkedin_url)
            if sig_date and len(text_clean) > 60:
                evidence.append({
                    "url": linkedin_url, "source": "linkedin",
                    "extracted_date": sig_date,
                    "extracted_text": _trim_sentences(
                        _build_evidence_text(text_clean, company, 480, icp_kw), 480),
                })
                found_sources.add("linkedin")
            return

        # --- Structured JSON from LinkedIn company profile ---
        about_text = _s(str(data.get("about", "")))
        tagline = _s(str(data.get("tagline", "")))

        profile_parts: List[str] = []
        for k in ("about", "tagline", "specialties", "industry",
                   "company_size", "headquarters", "founded", "type"):
            val = _s(str(data.get(k, "")))
            if val:
                profile_parts.append(f"{k}: {val}")
        funding_info = []
        for fk in ("total_funding_rounds", "last_round_funding",
                    "last_round_type", "last_round_investors"):
            fv = _s(str(data.get(fk, "")))
            if fv:
                funding_info.append(f"{fk}: {fv}")
        if funding_info:
            profile_parts.append("FUNDING: " + "; ".join(funding_info))
        products = data.get("product") or []
        if isinstance(products, list) and products:
            pnames = [_s(p.get("product_name", "")) for p in products
                      if isinstance(p, dict)]
            if pnames:
                profile_parts.append("PRODUCTS: " + ", ".join(pnames))

        # --- Resolve relative dates on updates and score by ICP relevance ---
        updates = data.get("updates") or []
        icp_kw_lower = {kw.lower() for kw in (icp_kw or set())}
        scored_updates: List[Tuple[float, Dict]] = []

        if isinstance(updates, list):
            for upd in updates:
                if not isinstance(upd, dict):
                    continue
                posted_raw = _s(upd.get("article_posted_date", ""))
                if posted_raw:
                    resolved = _parse_relative_date_from_api(posted_raw)
                    if resolved:
                        upd["_resolved_date"] = resolved
                txt = _s(upd.get("text", ""))
                if not txt or len(txt) < 20:
                    continue
                txt_lower = txt.lower()
                score = 0.0
                for kw in icp_kw_lower:
                    if len(kw) >= 4 and kw in txt_lower:
                        score += 5.0
                for anchor in EVIDENCE_ANCHORS:
                    if anchor in txt_lower:
                        score += 3.0
                for skw in SPECIFIC_INTENT_KW:
                    if skw in txt_lower:
                        score += 2.0
                if company.lower() in txt_lower:
                    score += 1.0
                if upd.get("_resolved_date"):
                    score += 2.0
                scored_updates.append((score, upd))

        scored_updates.sort(key=lambda x: x[0], reverse=True)

        # Build RECENT UPDATES section from top-ranked articles
        update_texts: List[str] = []
        for _, upd in scored_updates[:5]:
            txt = _s(upd.get("text", ""))
            posted = upd.get("_resolved_date") or _s(upd.get("article_posted_date", ""))
            link = _s(upd.get("article_link", ""))
            if txt:
                entry = f"[{posted}] {txt[:300]}"
                if link:
                    entry += f" (link: {link})"
                update_texts.append(entry)
        if update_texts:
            profile_parts.append(
                "RECENT UPDATES:\n" + "\n".join(update_texts))

        raw_profile_text = "\n".join(profile_parts)

        # --- LLM extraction with ICP context ---
        llm_result = _llm_extract_intent_from_data(
            raw_profile_text, "linkedin_profile", company,
            icp_text, icp_intent_signals, proxy_url,
            icp_ctx=icp_ctx)

        # --- Find best date and article link ---
        sig_date = None
        best_article_link = None

        if llm_result and llm_result.get("date"):
            sig_date = llm_result["date"]

        if not sig_date:
            for _, upd in scored_updates[:5]:
                resolved = upd.get("_resolved_date")
                if resolved:
                    d = _parse_iso_to_date(resolved)
                    if d and (date.today() - d).days <= 365:
                        sig_date = resolved
                        link = _s(upd.get("article_link", ""))
                        if link and link.startswith("http"):
                            best_article_link = link
                        break

        if not sig_date:
            for upd in (data.get("updates") or [])[:3]:
                if isinstance(upd, dict):
                    pd = _s(upd.get("article_posted_date", ""))
                    if pd:
                        d_try = _parse_iso_to_date(pd[:10])
                        if d_try and (date.today() - d_try).days <= 365:
                            sig_date = d_try.isoformat()
                            link = _s(upd.get("article_link", ""))
                            if link and link.startswith("http"):
                                best_article_link = link
                            break

        if not sig_date:
            sig_date = _extract_best_date(
                None, raw_profile_text[:4000], linkedin_url, strict=False)

        evidence_url = best_article_link or linkedin_url

        if llm_result and sig_date:
            evidence.append({
                "url": evidence_url,
                "source": "linkedin",
                "extracted_date": sig_date,
                "extracted_text": _trim_sentences(
                    llm_result["description"], 480),
            })
            found_sources.add("linkedin")
        elif sig_date and len(raw_profile_text) > 60:
            ev_text = _build_evidence_text(
                raw_profile_text, company, 480, icp_kw)
            if ev_text and len(ev_text) > 40:
                evidence.append({
                    "url": evidence_url, "source": "linkedin",
                    "extracted_date": sig_date,
                    "extracted_text": _trim_sentences(ev_text, 480),
                })
                found_sources.add("linkedin")

        # --- Fallback: about + top articles when LLM missed ICP signals ---
        if "linkedin" not in found_sources and about_text and len(about_text) > 40:
            about_lower = about_text.lower()
            about_has_icp = icp_kw_lower and any(
                kw in about_lower for kw in icp_kw_lower if len(kw) >= 4)
            about_has_intent = any(kw in about_lower for kw in SPECIFIC_INTENT_KW)
            tagline_lower = tagline.lower() if tagline else ""
            tagline_has_icp = icp_kw_lower and any(
                kw in tagline_lower for kw in icp_kw_lower if len(kw) >= 4)

            if about_has_icp or about_has_intent or tagline_has_icp:
                fallback_date = ""
                for _, upd in scored_updates[:3]:
                    rd = upd.get("_resolved_date")
                    if rd:
                        fallback_date = rd
                        break
                if fallback_date:
                    ev_text = _build_evidence_text(
                        about_text, company, 480, icp_kw)
                    if ev_text and len(ev_text) > 40:
                        evidence.append({
                            "url": linkedin_url,
                            "source": "linkedin",
                            "extracted_date": fallback_date,
                            "extracted_text": _trim_sentences(ev_text, 480),
                        })
                        found_sources.add("linkedin")

    def _probe_company_website():
        website = _s(lead.get("website"))
        if not website or _tl() < 1.5:
            return
        if not website.startswith("http"):
            website = "https://" + website
        base = website.rstrip("/")
        collected_text_parts: List[str] = []
        first_url = base
        for suffix in ("", "/about", "/news", "/careers", "/press"):
            if _tl() < 1.0:
                break
            url = f"{base}{suffix}"
            html = _scrape(url, proxy_url)
            if not html or len(html) < 200:
                continue
            text = _html_to_text(html)
            if text and len(text) > 80:
                collected_text_parts.append(
                    f"=== PAGE: {suffix or '/'} ===\n{text[:2000]}")
            if len(collected_text_parts) >= 3:
                break
        if not collected_text_parts:
            return
        combined = "\n\n".join(collected_text_parts)
        llm_result = _llm_extract_intent_from_data(
            combined, "company_website", company,
            icp_text, icp_intent_signals, proxy_url,
            icp_ctx=icp_ctx)
        if llm_result:
            sig_date = llm_result.get("date") or ""
            if not sig_date:
                sig_date = _extract_best_date(
                    None, combined[:4000], first_url)
            if not sig_date:
                sig_date = _extract_best_date(
                    None, combined[:4000], first_url, strict=False)
            if sig_date:
                real_dates = {d.isoformat() for d in _collect_dates_text(combined[:4000])}
                if sig_date not in real_dates:
                    ym = sig_date[:7]
                    if not any(ds.startswith(ym) for ds in real_dates):
                        sig_date = ""
            evidence.append({
                "url": first_url, "source": "company_website",
                "extracted_date": sig_date,
                "extracted_text": _trim_sentences(
                    llm_result["description"], 480),
            })
            found_sources.add("company_website")
        else:
            ev_text = _build_evidence_text(combined, company, 480, icp_kw)
            if ev_text and len(ev_text) > 40:
                sig_date = _extract_best_date(
                    None, combined[:4000], first_url)
                if not sig_date:
                    sig_date = _extract_best_date(
                        None, combined[:4000], first_url, strict=False)
                if sig_date:
                    real_dates = {d.isoformat() for d in _collect_dates_text(combined[:4000])}
                    if sig_date not in real_dates:
                        ym = sig_date[:7]
                        if not any(ds.startswith(ym) for ds in real_dates):
                            sig_date = ""
                evidence.append({
                    "url": first_url, "source": "company_website",
                    "extracted_date": sig_date,
                    "extracted_text": _trim_sentences(ev_text, 480),
                })
                found_sources.add("company_website")

    def _probe_news():
        if _tl() < 1.0 or "news" in found_sources or not proxy_url:
            return
        lead_country = _s(lead.get("country", ""))
        cc = lead_country[:2].lower() if lead_country else "us"
        q_terms = _icp_queries[:3] if _icp_queries else []
        if not q_terms:
            intent_kw = " ".join((icp_intent_signals or [])[:3]).lower() or "funding hiring"
            q_terms = [intent_kw]
        articles: List[Dict] = []
        for qt in q_terms[:2]:
            query = f"{company} {qt}"
            articles = _fetch_google_news_sd(query, proxy_url, country=cc)
            if articles:
                break
        if not articles:
            articles = _fetch_google_news_sd(company, proxy_url, country=cc)
        if not articles:
            return

        icp_kw_lower = {kw.lower() for kw in (icp_kw or set())}
        company_lower = company.lower()

        scored_articles: List[Tuple[float, Dict, str]] = []
        for art in articles[:8]:
            title = _s(art.get("title", ""))
            snippet = _s(art.get("snippet", ""))
            source_name = _s(art.get("source", ""))
            last_upd = _s(art.get("lastUpdated") or art.get("last_updated", ""))
            url = _s(art.get("url", ""))

            parsed_date = ""
            if last_upd:
                dates_found = _collect_dates_text(last_upd)
                if dates_found:
                    parsed_date = max(dates_found).isoformat()
                else:
                    d_try = _parse_iso_to_date(last_upd[:10])
                    if d_try:
                        parsed_date = d_try.isoformat()

            combined_text = f"{title} {snippet}".lower()
            score = 0.0
            for kw in icp_kw_lower:
                if len(kw) >= 4 and kw in combined_text:
                    score += 5.0
            for anchor in EVIDENCE_ANCHORS:
                if anchor in combined_text:
                    score += 3.0
            for skw in SPECIFIC_INTENT_KW:
                if skw in combined_text:
                    score += 2.0
            if company_lower in combined_text:
                score += 3.0
            if parsed_date:
                try:
                    d = date.fromisoformat(parsed_date)
                    age_days = (date.today() - d).days
                    if age_days <= 60:
                        score += 4.0
                    elif age_days <= 365:
                        score += 2.0
                except Exception:
                    pass

            art["_parsed_date"] = parsed_date
            art["_combined_text"] = f"[{source_name}] {title}: {snippet}" if snippet else f"[{source_name}] {title}"
            scored_articles.append((score, art, url))

        scored_articles.sort(key=lambda x: x[0], reverse=True)

        best_url = ""
        best_date = ""
        for _, art, url in scored_articles:
            if not best_url and url and url.startswith("http"):
                best_url = url
            pd = art.get("_parsed_date", "")
            if pd and not best_date:
                best_date = pd
            if best_url and best_date:
                break

        for sc, art, url in scored_articles[:1]:
            pd = art.get("_parsed_date", "")
            if url and url.startswith("http") and pd:
                best_url = url
                best_date = pd

        news_parts: List[str] = []
        for _, art, _ in scored_articles[:8]:
            entry = art.get("_combined_text", "")
            pd = art.get("_parsed_date", "")
            if pd:
                entry += f" ({pd})"
            if entry:
                news_parts.append(entry)

        if not news_parts:
            return

        top_score, top_art, top_url = scored_articles[0]
        top_date = top_art.get("_parsed_date", "")

        ev_url = best_url
        if top_url and top_url.startswith("http"):
            ev_url = top_url

        scraped_article = ""
        if _tl() > 3.0 and ev_url and ev_url.startswith("http"):
            try:
                html = _scrape(ev_url, proxy_url)
                if html and len(html) > 200:
                    scraped_article = _html_to_text(html)[:2000]
            except Exception:
                pass

        combined = "\n".join(news_parts)
        if scraped_article and len(scraped_article) > 100:
            combined = f"=== FULL ARTICLE ===\n{scraped_article}\n\n=== OTHER NEWS ===\n{combined}"

        llm_result = _llm_extract_intent_from_data(
            combined, "news", company,
            icp_text, icp_intent_signals, proxy_url,
            icp_ctx=icp_ctx)
        if llm_result:
            sig_date = llm_result.get("date") or top_date or best_date
            ev_text = llm_result["description"]
            if company_lower not in ev_text.lower() and company:
                ev_text = f"{company}: {ev_text}"
            evidence.append({
                "url": ev_url or f"https://news.google.com/search?q={quote(company)}",
                "source": "news",
                "extracted_date": sig_date,
                "extracted_text": _trim_sentences(ev_text, 480),
            })
            found_sources.add("news")
        elif (top_date or best_date) and combined and len(combined) > 60:
            ev_text = _build_evidence_text(combined, company, 480, icp_kw)
            if ev_text and len(ev_text) > 40:
                evidence.append({
                    "url": ev_url or f"https://news.google.com/search?q={quote(company)}",
                    "source": "news",
                    "extracted_date": top_date or best_date,
                    "extracted_text": _trim_sentences(ev_text, 480),
                })
                found_sources.add("news")

    def _probe_wikipedia():
        if _tl() < 1.5 or "wikipedia" in found_sources:
            return
        wiki = _fetch_wikipedia(company)
        if not wiki or not wiki.get("url"):
            return
        text = wiki.get("text", "")
        if len(text) < 100:
            return
        url = wiki["url"]
        sig_date = _extract_best_date(None, text[:6000], url)
        if not sig_date:
            sig_date = _extract_best_date(None, text[:6000], url, strict=False)
        if not sig_date:
            return
        extracted = _build_evidence_text(text, company, 480, icp_kw)
        if not extracted or len(extracted) < 40:
            return
        if not any(kw in extracted.lower() for kw in SPECIFIC_INTENT_KW):
            return
        evidence.append({
            "url": url,
            "source": "wikipedia",
            "extracted_date": sig_date,
            "extracted_text": _trim_sentences(extracted, 480),
        })
        found_sources.add("wikipedia")

    def _probe_linkedin_jobs():
        if _tl() < 2.0 or not proxy_url:
            return
        state = _s(lead.get("state", ""))
        country = _s(lead.get("country", ""))
        loc_parts = [p for p in (state, country) if p]
        loc_str = ", ".join(loc_parts) if loc_parts else ""

        queries: List[str] = []
        for role in _target_roles[:2]:
            q = f"{role} at {company}"
            if loc_str:
                q += f" in {loc_str}"
            queries.append(q)
        for qt in _icp_queries[:2]:
            queries.append(f"{qt} at {company}" + (f" in {loc_str}" if loc_str else ""))
        broad_q = f"{company} hiring" + (f" in {loc_str}" if loc_str else "")
        queries.append(broad_q)

        company_lower = company.lower()
        company_words = set(re.split(r'\s+', company_lower))
        icp_kw_lower = {kw.lower() for kw in (icp_kw or set())}
        roles_lower = [r.lower() for r in _target_roles]

        all_matched: List[Tuple[float, Dict]] = []

        for field in queries:
            if _tl() < 1.5:
                break
            for j in (_fetch_linkedin_jobs(field, proxy_url) or [])[:10]:
                if not isinstance(j, dict):
                    continue
                co = _s(j.get("company_name", "")).lower()
                if company_lower not in co and co not in company_lower:
                    co_words = set(re.split(r'[\s()+,]+', co))
                    if not company_words & co_words:
                        continue
                title = _s(j.get("job_position") or j.get("title", ""))
                url = _s(j.get("job_link", ""))
                posted = _s(j.get("job_posting_date", ""))[:10]
                if not title or not url or not url.startswith("http"):
                    continue
                if not posted or len(posted) < 10:
                    continue
                d = _parse_iso_to_date(posted)
                if not d or (date.today() - d).days > 365:
                    continue

                title_lower = title.lower()
                score = 0.0
                for role in roles_lower:
                    role_words = set(re.split(r'\s+', role))
                    title_words = set(re.split(r'[\s,()]+', title_lower))
                    overlap = role_words & title_words
                    if len(overlap) >= max(1, len(role_words) // 2):
                        score += 10.0
                        break
                for kw in icp_kw_lower:
                    if len(kw) >= 4 and kw in title_lower:
                        score += 5.0
                for skw in SPECIFIC_INTENT_KW:
                    if skw in title_lower:
                        score += 2.0
                age_days = (date.today() - d).days
                if age_days <= 30:
                    score += 4.0
                elif age_days <= 90:
                    score += 2.0

                all_matched.append((score, {
                    "title": title, "url": url, "posted": posted,
                    "location": _s(j.get("job_location", "")),
                    "company_name": _s(j.get("company_name", "")),
                }))

        if not all_matched:
            return

        all_matched.sort(key=lambda x: x[0], reverse=True)

        best_score, best = all_matched[0]
        best_url = best["url"]
        best_date = best["posted"]

        jobs_summary_parts = []
        for sc, j in all_matched[:8]:
            entry = f"- {j['company_name']}: {j['title']} in {j['location']} (posted {j['posted']})"
            jobs_summary_parts.append(entry)
        jobs_summary = "\n".join(jobs_summary_parts)

        if _tl() > 2.0:
            llm_result = _llm_extract_intent_from_data(
                jobs_summary, "linkedin_jobs", company,
                icp_text, icp_intent_signals, proxy_url,
                icp_ctx=icp_ctx)
            if llm_result:
                ev_text = llm_result["description"]
                if best["company_name"] and best["company_name"].lower() not in ev_text.lower():
                    ev_text = f"{best['company_name']}: {ev_text}"
                evidence.append({
                    "url": best_url, "source": "linkedin",
                    "_probe": "linkedin_jobs",
                    "extracted_date": best_date,
                    "extracted_text": _trim_sentences(ev_text, 480),
                })
                found_sources.add("linkedin")
                return

        ev_parts: List[str] = []
        ev_parts.append(f"{best['company_name']} hiring {best['title']}")
        if best["location"]:
            ev_parts[-1] += f" in {best['location']}"
        ev_parts[-1] += f" (posted {best_date})"

        for _, job in all_matched[1:3]:
            extra = f"Also hiring {job['title']}"
            if job["location"]:
                extra += f" in {job['location']}"
            extra += f" (posted {job['posted']})"
            ev_parts.append(extra)

        ev_text = ". ".join(ev_parts)
        evidence.append({
            "url": best_url, "source": "linkedin",
            "_probe": "linkedin_jobs",
            "extracted_date": best_date,
            "extracted_text": _trim_sentences(ev_text, 480),
        })
        found_sources.add("linkedin")

    def _probe_google_jobs():
        if _tl() < 2.0 or not proxy_url:
            return
        state = _s(lead.get("state", ""))
        lead_country = _s(lead.get("country", ""))
        loc_parts = [p for p in (state, lead_country) if p]
        loc_str = ", ".join(loc_parts) if loc_parts else ""
        cc = lead_country[:2].lower() if lead_country else "us"

        queries: List[str] = []
        for role in _target_roles[:2]:
            q = f"{role} at {company}"
            if loc_str:
                q += f" in {loc_str}"
            queries.append(q)
        for qt in _icp_queries[:2]:
            queries.append(f"{qt} at {company}" + (f" in {loc_str}" if loc_str else ""))
        broad_q = f"{company} hiring" + (f" in {loc_str}" if loc_str else "")
        queries.append(broad_q)

        company_lower = company.lower()
        company_words = set(re.split(r'\s+', company_lower))
        icp_kw_lower = {kw.lower() for kw in (icp_kw or set())}
        roles_lower = [r.lower() for r in _target_roles]

        all_matched: List[Tuple[float, Dict]] = []

        for query in queries:
            if _tl() < 1.5:
                break
            for j in (_fetch_google_jobs(query, proxy_url, country=cc) or [])[:10]:
                if not isinstance(j, dict):
                    continue
                co = _s(j.get("company_name", "")).lower()
                if company_lower not in co and co not in company_lower:
                    co_words = set(re.split(r'[\s()+,]+', co))
                    if not company_words & co_words:
                        continue
                title = _s(j.get("title", ""))
                if not title:
                    continue

                url = ""
                for ao in (j.get("apply_options") or j.get("apply_links") or [])[:8]:
                    lk = _s(ao.get("link", ""))
                    if lk and lk.startswith("http"):
                        if "linkedin.com" in lk.lower():
                            url = lk
                            break
                        if not url:
                            url = lk
                if not url:
                    url = _s(j.get("share_link") or j.get("url", ""))
                if not url or not url.startswith("http"):
                    continue

                location = _s(j.get("location", ""))
                desc = _s(j.get("description", ""))

                sig_date = ""
                exts = j.get("extensions") or []
                for ext in exts:
                    ext_s = _s(str(ext)).strip()
                    resolved = _parse_relative_date_from_api(ext_s)
                    if resolved:
                        sig_date = resolved
                        break
                if not sig_date and desc:
                    dl = _collect_dates_text(desc[:3000])
                    if dl:
                        sig_date = max(dl).isoformat()

                highlights_text = ""
                for hl in (j.get("job_highlights") or []):
                    if not isinstance(hl, dict):
                        continue
                    hl_title = _s(hl.get("title", ""))
                    items = hl.get("items") or []
                    if hl_title.lower() in ("responsibilities", "qualifications"):
                        for item in items[:5]:
                            highlights_text += f" {_s(str(item))}"

                title_lower = title.lower()
                combined_text = f"{title_lower} {desc[:500].lower()} {highlights_text[:500].lower()}"
                score = 0.0
                for role in roles_lower:
                    role_words = set(re.split(r'\s+', role))
                    title_words = set(re.split(r'[\s,()]+', title_lower))
                    overlap = role_words & title_words
                    if len(overlap) >= max(1, len(role_words) // 2):
                        score += 10.0
                        break
                for kw in icp_kw_lower:
                    if len(kw) >= 4 and kw in combined_text:
                        score += 5.0
                for skw in SPECIFIC_INTENT_KW:
                    if skw in combined_text:
                        score += 2.0
                if sig_date:
                    try:
                        d = date.fromisoformat(sig_date)
                        age_days = (date.today() - d).days
                        if age_days <= 30:
                            score += 4.0
                        elif age_days <= 90:
                            score += 2.0
                    except Exception:
                        pass

                all_matched.append((score, {
                    "title": title, "url": url, "date": sig_date,
                    "location": location,
                    "company_name": _s(j.get("company_name", "")),
                    "desc": desc[:1500],
                    "highlights": highlights_text[:1000],
                }))

        if not all_matched:
            return

        all_matched.sort(key=lambda x: x[0], reverse=True)

        best_score, best = all_matched[0]
        best_url = best["url"]
        best_date = best["date"]

        if _tl() > 2.0 and (best["desc"] or best["highlights"]):
            llm_input = f"TOP JOB: {best['company_name']} - {best['title']} in {best['location']}"
            if best_date:
                llm_input += f" (posted {best_date})"
            llm_input += "\n"
            if best["desc"]:
                llm_input += f"DESCRIPTION:\n{best['desc'][:1200]}\n"
            if best["highlights"]:
                llm_input += f"KEY REQUIREMENTS:\n{best['highlights'][:800]}\n"
            if len(all_matched) > 1:
                llm_input += f"\nALL {len(all_matched)} OPEN ROLES AT {company}:\n"
                for _, j2 in all_matched[1:6]:
                    line = f"- {j2['title']} in {j2['location']}"
                    if j2.get("date"):
                        line += f" (posted {j2['date']})"
                    llm_input += line + "\n"

            llm_result = _llm_extract_intent_from_data(
                llm_input, "job_board", company,
                icp_text, icp_intent_signals, proxy_url,
                icp_ctx=icp_ctx)
            if llm_result:
                ev_text = llm_result["description"]
                if company_lower not in ev_text.lower():
                    ev_text = f"{best['company_name']}: {ev_text}"
                llm_date = llm_result.get("date") or ""
                if llm_date and not best_date:
                    best_date = llm_date
                evidence.append({
                    "url": best_url, "source": "job_board",
                    "_probe": "google_jobs",
                    "extracted_date": best_date,
                    "extracted_text": _trim_sentences(ev_text, 480),
                })
                found_sources.add("job_board")
                return

        ev_parts: List[str] = []
        ev_parts.append(f"{best['company_name']} hiring {best['title']}")
        if best["location"]:
            ev_parts[-1] += f" in {best['location']}"
        if best_date:
            ev_parts[-1] += f" (posted {best_date})"

        for _, job in all_matched[1:3]:
            extra = f"Also hiring {job['title']}"
            if job["location"]:
                extra += f" in {job['location']}"
            if job["date"]:
                extra += f" (posted {job['date']})"
            ev_parts.append(extra)

        ev_text = ". ".join(ev_parts)
        if len(ev_text) < 30:
            return
        evidence.append({
            "url": best_url, "source": "job_board",
            "_probe": "google_jobs",
            "extracted_date": best_date,
            "extracted_text": _trim_sentences(ev_text, 480),
        })
        found_sources.add("job_board")

    def _probe_duckduckgo():
        if _tl() < 2.0 or not proxy_url:
            return
        sig_terms = [_s(s) for s in (icp_intent_signals or [])[:2] if _s(s)]
        queries = []
        if _icp_queries:
            queries.extend(_icp_queries[:2])
        kw = " ".join(list(icp_kw)[:3]) if icp_kw else ""
        if kw:
            queries.append(f'"{company}" {kw}')
        for st in sig_terms:
            queries.append(f'"{company}" {st}')
        if not queries:
            queries.append(f'"{company}" hiring funding expansion')
        seen_urls: Set[str] = set()
        for q in queries[:3]:
            if _tl() < 1.5:
                break
            for item in (_fetch_duckduckgo(q, proxy_url) or [])[:5]:
                if _tl() < 1.0 or not isinstance(item, dict):
                    continue
                url = _s(item.get("link", ""))
                if not url or not url.startswith("http") or url in seen_urls:
                    continue
                seen_urls.add(url)
                domain = urlparse(url.lower()).netloc
                if any(d in domain for d in BLOCKED_DOMAINS):
                    continue
                html = _scrape(url, proxy_url)
                source = _classify_source(url)
                ev = _extract_evidence_item(url, html, company, source,
                                            icp_keywords=icp_kw)
                if ev:
                    ev["_probe"] = "duckduckgo"
                    evidence.append(ev)
                    found_sources.add(source)
                    return

    if _tl() > 1.5:
        try:
            _probe_company_website()
        except Exception:
            pass
    print(f"[SENOO] after website probe: {len(evidence)} evidence, "
          f"sources={found_sources}, tl={_tl():.1f}s", flush=True)

    _api_probes = [
        _probe_linkedin,
        _probe_linkedin_jobs,
        _probe_google_jobs,
        _probe_news,
        _probe_duckduckgo,
    ]

    remaining = _tl()
    if remaining > 2.0 and _api_probes:
        try:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futs = {executor.submit(p): p.__name__ for p in _api_probes}
                for f in as_completed(futs, timeout=max(remaining - 1.0, 2.0)):
                    try:
                        f.result()
                    except Exception as _e:
                        print(f"[SENOO] probe {futs.get(f, '?')} error: {_e}",
                              flush=True)
        except Exception:
            pass
    print(f"[SENOO] after all probes: {len(evidence)} evidence, "
          f"sources={found_sources}, tl={_tl():.1f}s", flush=True)

    if not evidence and _tl() > 1.0:
        _probe_wikipedia()

    if evidence:
        for ev in evidence:
            ev["_quality_score"] = _score_evidence_item(ev, company, icp_kw)
        evidence = [ev for ev in evidence
                    if ev.get("_date_status") != "fabricated"
                    and ev.get("_quality_score", 0) > 0]
        evidence.sort(key=lambda e: e.get("_quality_score", 0), reverse=True)

    return evidence

def _compute_intent_features(evidence: List[Dict]) -> Dict[str, float]:
    features: Dict[str, float] = {}
    source_keys = ["linkedin", "job_board", "github",
                   "company_website", "news", "wikipedia"]
    by_source: Dict[str, List[Dict]] = {}
    for item in evidence:
        by_source.setdefault(item.get("source", "other"), []).append(item)

    weighted_sum = 0.0
    newest_date: Optional[date] = None

    for sk in source_keys:
        items = by_source.get(sk, [])
        present = 1.0 if items else 0.0
        features[f"{sk}_present"] = present
        quality = 0.0
        if items:
            best = items[0]
            txt = best.get("extracted_text", "")
            length_score = min(1.0, len(txt) / 400.0)
            tl = txt.lower()
            anchor_count = sum(1 for a in EVIDENCE_ANCHORS if a in tl)
            anchor_score = min(1.0, anchor_count / 3.0)
            quality = 0.5 * length_score + 0.5 * anchor_score
            try:
                d = date.fromisoformat(best.get("extracted_date", ""))
                if newest_date is None or d > newest_date:
                    newest_date = d
            except Exception:
                pass
        features[f"{sk}_quality"] = quality
        weighted_sum += present * quality * SOURCE_MULTIPLIERS.get(sk, 0.3)

    if newest_date:
        age_months = (date.today() - newest_date).days / 30.0
        features["recency_score"] = (1.0 if age_months <= 2
                                     else 0.5 if age_months <= 12
                                     else 0.25)
    else:
        features["recency_score"] = 0.0
    features["source_multiplier_weighted_sum"] = weighted_sum
    features["total_sources"] = float(
        sum(1 for sk in source_keys if by_source.get(sk)))
    return features

def _llm_verify_candidate(company: str, icp_prompt: str,
                          buyer_desc: str,
                          evidence: List[Dict],
                          proxy_url: str,
                          icp_intent_signals: Optional[List[str]] = None
                          ) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "raw_llm": 0.0, "confidence": 0,
        "chosen_url": "", "chosen_excerpt": "",
        "verified_description": "",
        "source_mult": 0.3, "time_decay": 0.25,
        "intent_final": 0.0, "best_date": "",
    }
    if not evidence:
        return result

    best_date_for_check = ""
    for ev in evidence[:5]:
        d = ev.get("extracted_date", "")
        if d:
            best_date_for_check = d
            break

    ev_text = ""
    for i, ev in enumerate(evidence[:5], 1):
        ev_text += (
            f"\nEvidence #{i}:\n"
            f"  Source: {ev.get('source', 'unknown')}\n"
            f"  URL: {ev.get('url', 'N/A')}\n"
            f"  Date: {ev.get('extracted_date', 'unknown')}\n"
            f"  Text: {ev.get('extracted_text', '')[:400]}\n"
        )

    intent_section = ""
    if icp_intent_signals:
        sigs = [str(s) for s in icp_intent_signals[:5] if isinstance(s, str) and str(s).strip()]
        if sigs:
            intent_section = f"\nICP INTENT SIGNALS TO MATCH: {', '.join(sigs)}\n(Prioritize evidence that matches these signals.)"
    prompt = f"""You are verifying intent evidence for a B2B lead qualification system.

COMPANY: {company}
ICP PROMPT: {icp_prompt[:800]}
BUYER DESCRIPTION: {buyer_desc[:300]}{intent_section}

COLLECTED EVIDENCE:
{ev_text}

TASK: Evaluate this evidence and determine:
1. Does the evidence show SPECIFIC intent (hiring, funding, product launch, expansion, partnership, etc.)?
2. Does the evidence match the ICP intent signals above?
3. Does the company match the ICP criteria?
4. Is the evidence date recent and verifiable?

REJECT generic claims like "actively operating" or "market activity" or company taglines.

CRITICAL RULES for chosen_excerpt and verified_description:
- chosen_excerpt: Copy VERBATIM 1-2 sentences directly from the evidence text above. Do NOT paraphrase.
- verified_description: Copy the MOST RELEVANT 2-3 sentences VERBATIM from the evidence text that demonstrate the specific intent action (hiring, funding, launch, etc.). Do NOT rephrase, summarize, or interpret. Use the exact words from the evidence.
- NEVER generate new text or interpretations.
- NEVER include phrases like "indicating growth", "potential expansion", "does not directly connect".
- NEVER reference or echo back the ICP prompt terms in your own words.
- claim_supported: Does the evidence TEXT actually support the claim? If the text is real content but does NOT support specific intent, set false.

DATE VERIFICATION (THREE possible outcomes for date_status):
- "verified": Evidence has a SPECIFIC date/timestamp (month+day) that matches the evidence date.
- "no_date": Evidence genuinely has NO dates/timestamps, or only bare years. Cannot verify date.
- "fabricated": Dates in evidence CONTRADICT the claimed date, OR date shows manufactured precision (e.g., only "2025" in text but date is "2025-01-01"), OR date comes from copyright footer / founding year.
NOTE: copyright "© 2024" and "Founded in 2015" are NOT intent dates — mark as "fabricated" if those are the only date references.

Respond with ONLY JSON (no markdown):
{{"raw_score": 0-50, "confidence": 0-100, "chosen_url": "best evidence URL", "chosen_excerpt": "VERBATIM 1-2 sentences from the evidence text", "verified_description": "VERBATIM 2-3 sentences from evidence showing the specific intent action", "date_status": "verified"|"no_date"|"fabricated", "claim_supported": true/false}}
"""

    response_text = _openrouter_chat(prompt, proxy_url)
    if not response_text:
        return result

    try:
        resp = response_text.strip()
        if resp.startswith("```"):
            resp = re.sub(r'^```(?:json)?\s*', '', resp)
            resp = re.sub(r'\s*```$', '', resp)
        parsed = json.loads(resp)
    except json.JSONDecodeError:
        return result

    raw_llm = min(50.0, max(0.0, float(parsed.get("raw_score", 0))))
    confidence = min(100, max(0, int(parsed.get("confidence", 0))))
    chosen_url = _s(parsed.get("chosen_url"))
    chosen_excerpt = _s(parsed.get("chosen_excerpt"))
    verified_desc = _s(parsed.get("verified_description"))
    claim_supported = parsed.get("claim_supported", True)

    date_status = parsed.get("date_status")
    if date_status is None:
        legacy = parsed.get("date_verified", True)
        date_status = "verified" if legacy else "no_date"
    if date_status not in ("verified", "no_date", "fabricated"):
        date_status = "verified"

    if date_status != "fabricated" and best_date_for_check:
        ev_content = ""
        for ev in evidence[:3]:
            ev_content += " " + (ev.get("extracted_text") or "")
        if ev_content.strip():
            stripped = _strip_non_intent_dates(ev_content)
            precision = _check_date_in_content(best_date_for_check, stripped)
            if precision == "year_only" and date_status == "verified":
                date_status = "no_date"
                confidence = min(confidence, 50)
            elif precision == "no_match" and date_status == "verified":
                date_status = "fabricated"
                confidence = 0

    if date_status == "fabricated":
        confidence = 0
    elif date_status == "no_date":
        confidence = min(confidence, 50)

    if not claim_supported and date_status != "fabricated":
        confidence = 0
        date_status = "fabricated"

    source_mult = 0.3
    _PREFERRED_SOURCES = ("linkedin", "job_board")
    if chosen_url:
        src_type = _classify_source(chosen_url)
        source_mult = SOURCE_MULTIPLIERS.get(src_type, 0.3)
        if src_type not in _PREFERRED_SOURCES:
            for ev in evidence:
                ev_src = ev.get("source") or _classify_source(ev.get("url", ""))
                if ev_src in _PREFERRED_SOURCES:
                    better_m = SOURCE_MULTIPLIERS.get(ev_src, 0.3)
                    if better_m > source_mult:
                        source_mult = better_m
                        chosen_url = ev.get("url") or chosen_url
                        break
    else:
        for ev in evidence:
            m = SOURCE_MULTIPLIERS.get(ev.get("source", "other"), 0.3)
            if m > source_mult:
                source_mult = m
                chosen_url = ev.get("url", "")

    time_decay = 0.25
    best_date = ""
    for ev in evidence:
        if ev.get("url") == chosen_url or not best_date:
            d_str = ev.get("extracted_date", "")
            if d_str:
                best_date = d_str
    if best_date:
        try:
            sig_date = date.fromisoformat(best_date)
            age_months = (date.today() - sig_date).days / 30.0
            time_decay = (1.0 if age_months <= 2
                          else 0.5 if age_months <= 12
                          else 0.25)
        except Exception:
            pass

    intent_final = raw_llm * (confidence / 100.0) * source_mult * time_decay

    result.update({
        "raw_llm": raw_llm,
        "confidence": confidence,
        "chosen_url": chosen_url,
        "chosen_excerpt": chosen_excerpt,
        "verified_description": verified_desc,
        "source_mult": source_mult,
        "time_decay": time_decay,
        "intent_final": intent_final,
        "best_date": best_date,
    })
    return result

_GENERIC_INTENT_RX = [
    re.compile(r'is\s+actively\s+operating\s+in\s+\w+', re.I),
    re.compile(r'visible\s+market\s+activity', re.I),
    re.compile(r'market\s+activity\s+and\s+company\s+updates', re.I),
    re.compile(r'business\s+operations\s+and\s+updates', re.I),
    re.compile(r'^.{0,50}\s+is\s+(?:actively\s+)?(?:operating|expanding|growing)', re.I),
    re.compile(r'company\s+(?:updates|activities|operations)', re.I),
    re.compile(r'market\s+(?:activity|presence|operations)', re.I),
]
_TEMPLATED_RX = re.compile(
    r'^\w+(?:\s+\w+){0,3}\s+is\s+\w+ing\s+(?:in\s+)?\w+\s*\.?$', re.I)

def _best_evidence_sentence_with_anchor(evidence: List[Dict], company: str) -> str:
    company_l = company.lower() if company else ""
    best = ""
    best_score = -1
    for ev in evidence or []:
        text = ev.get("extracted_text", "") or ""
        for sent in re.split(r'(?<=[.!?])\s+', text):
            sent = sent.strip()
            if len(sent) < 50 or len(sent) > 500:
                continue
            sl = sent.lower()
            has_anchor = any(a in sl for a in EVIDENCE_ANCHORS)
            has_company = company_l and company_l in sl
            score = (2.0 if has_anchor else 0) + (2.0 if has_company else 0)
            if score > best_score and has_anchor:
                best_score = score
                best = sent[:499]
    return best

def _validator_safe_description(desc: str, company: str, sig_date: str,
                                evidence: Optional[List[Dict]],
                                ctx: Optional[Dict] = None) -> str:
    if not desc or _is_generic_description(desc):
        fallback = _best_evidence_sentence_with_anchor(evidence or [], company)
        if fallback and not _is_generic_description(fallback):
            desc = fallback

    dl = (desc or "").lower()
    has_action = any(kw in dl for kw in SPECIFIC_INTENT_KW)

    if _is_generic_description(desc or "") or not has_action:
        for ev in sorted((evidence or []),
                         key=lambda e: -e.get("_quality_score", 0)):
            et = ev.get("extracted_text", "")
            if et and len(et) >= 50 and not _is_generic_description(et):
                desc = et[:499]
                break

    dl = (desc or "").lower()
    if not any(kw in dl for kw in SPECIFIC_INTENT_KW):
        for ev in (evidence or []):
            et = ev.get("extracted_text", "")
            if et and len(et) >= 50:
                desc = et[:499]
                break

    if desc and company and company.lower() not in (desc or "").lower():
        desc = f"{company}: {desc}"

    return desc[:499] if desc else ""

def _build_output(lead: Dict, ctx: Dict,
                  verification: Dict,
                  evidence: Optional[List[Dict]] = None) -> Optional[Dict]:

    lead_id = lead.get("id")
    if lead_id is None:
        return None

    business = _s(lead.get("business"))
    if not business:
        return None
    lead_industry = _s(lead.get("industry"))
    lead_sub = _s(lead.get("sub_industry"))
    if not lead_industry or not lead_sub:
        return None
    role = _s(lead.get("role"))
    if not role:
        return None

    seniority = _infer_seniority(role, _s(lead.get("role_type")))
    if seniority not in SENIORITY_ENUM:
        seniority = _SEN_DEFAULT

    website = _s(lead.get("website") or lead.get("company_website"))
    if website and not website.startswith(("http://", "https://")):
        website = "https://" + website

    linkedin = _s(lead.get("company_linkedin"))
    if linkedin and not linkedin.startswith(("http://", "https://")):
        linkedin = "https://" + linkedin

    country = _s(lead.get("country"))
    if not country:
        return None

    def _rank_evidence(ev: Dict) -> float:
        sc = 0.0
        url = ev.get("url", "")
        if not url or not url.startswith(("http://", "https://")):
            return -1.0
        txt = ev.get("extracted_text", "")
        sc += _snippet_concreteness_score(txt, business) * 0.40
        src = ev.get("source", "other")
        sc += SOURCE_MULTIPLIERS.get(src, 0.3) * 0.25
        d_str = ev.get("extracted_date", "")
        if d_str:
            try:
                d = date.fromisoformat(d_str)
                age = (date.today() - d).days
                sc += (0.25 if age <= 60 else 0.15 if age <= 365 else 0.03)
            except Exception:
                sc += 0.03
        if len(txt) >= 100:
            sc += 0.05
        if len(txt) >= 200:
            sc += 0.05
        return sc

    _ev_url = ""
    _ev_date = ""
    _ev_text = ""
    ranked_ev: List[Dict] = []
    if evidence:
        ranked_ev = sorted(evidence, key=_rank_evidence, reverse=True)
        for ev in ranked_ev:
            eu = ev.get("url", "")
            if eu and eu.startswith(("http://", "https://")):
                _ev_url = eu
                _ev_date = ev.get("extracted_date", "")
                _ev_text = ev.get("extracted_text", "")
                break

    chosen_url = verification.get("chosen_url", "")
    if not chosen_url or not chosen_url.startswith(("http://", "https://")):
        chosen_url = _ev_url
    if not chosen_url:
        return None

    _no_ev_phrases = ("no specific intent", "no relevant intent",
                      "no relevant evidence", "no evidence found",
                      "no intent evidence", "does not indicate",
                      "does not suggest", "no direct evidence",
                      "no clear evidence", "unable to find",
                      "lacks specific", "no specific indication",
                      "no clear indication", "unable to verify",
                      "limited evidence", "insufficient evidence",
                      "does not show", "not enough evidence",
                      "no evidence of", "no indication of",
                      "no signs of", "does not appear",
                      "no mention of", "no data on",
                      "does not directly connect",
                      "does not specifically connect",
                      "does not provide evidence",
                      "does not clearly",
                      "no clear connection",
                      "not directly related",
                      "not specifically related")
    def _has_negative_evidence(text: str) -> bool:
        tl = text.lower()
        return any(p in tl for p in _no_ev_phrases)

    desc = _ev_text
    if desc and _has_negative_evidence(desc):
        desc = ""
    if not desc or len(desc) < 50:
        for ev in ranked_ev:
            et = ev.get("extracted_text", "")
            if et and len(et) >= 50 and not _has_negative_evidence(et):
                desc = et
                break
    if not desc or len(desc) < 50:
        chosen_excerpt = _s(verification.get("chosen_excerpt", ""))
        if chosen_excerpt and len(chosen_excerpt) >= 50 and not _has_negative_evidence(chosen_excerpt):
            desc = chosen_excerpt
    if not desc or len(desc) < 50:
        verified_desc = _s(verification.get("verified_description", ""))
        if verified_desc and len(verified_desc) >= 50 and not _has_negative_evidence(verified_desc):
            desc = verified_desc
    if not desc or len(desc) < 30:
        return None

    sig_date = verification.get("best_date", "")
    if not sig_date:
        sig_date = _ev_date
    if not sig_date and evidence:
        for ev in ranked_ev:
            if ev.get("url", "") == chosen_url:
                d = ev.get("extracted_date", "")
                if d:
                    sig_date = d
                    break
    if not sig_date and evidence:
        for ev in ranked_ev:
            d = ev.get("extracted_date", "")
            if d:
                sig_date = d
                break
    intent_source = _classify_source(chosen_url)
    if intent_source not in INTENT_SOURCE_ENUM:
        intent_source = "company_website"

    if not sig_date:
        return None

    desc = _validator_safe_description(desc, business, sig_date, evidence, ctx=ctx)
    if not desc or len(desc) < 30:
        return None

    if _is_generic_description(desc):
        better = ""
        for ev in ranked_ev:
            et = ev.get("extracted_text", "")
            if et and len(et) >= 50 and not _is_generic_description(et):
                better = et[:499]
                break
        if better:
            desc = better
        else:
            return None

    if sig_date and desc:
        precision = _check_date_in_content(sig_date,
                                           _strip_non_intent_dates(desc))
        if precision in ("no_match", "year_only"):
            _found_better = False
            for ev in ranked_ev:
                et = ev.get("extracted_text", "")
                if et:
                    p2 = _check_date_in_content(
                        sig_date, _strip_non_intent_dates(et))
                    if p2 in ("verified", "approximate"):
                        _found_better = True
                        break
            if not _found_better:
                for ev in ranked_ev:
                    d2 = ev.get("extracted_date", "")
                    if d2 and d2 != sig_date:
                        et2 = ev.get("extracted_text", "")
                        if et2:
                            p3 = _check_date_in_content(
                                d2, _strip_non_intent_dates(et2))
                            if p3 in ("verified", "approximate"):
                                sig_date = d2
                                _found_better = True
                                break
            if not _found_better and intent_source in _SOURCES_DATE_REQUIRED:
                return None

    primary_snippet = ""
    if ranked_ev:
        for ev in ranked_ev:
            et = ev.get("extracted_text", "")
            if et and len(et) > 30:
                primary_snippet = et[:999]
                break
    if not primary_snippet or len(primary_snippet.strip()) < 10:
        chosen_excerpt = _s(verification.get("chosen_excerpt", ""))
        if chosen_excerpt and len(chosen_excerpt) > 20:
            primary_snippet = chosen_excerpt[:999]
    if not primary_snippet or len(primary_snippet.strip()) < 10:
        primary_snippet = desc[:999] if desc else chosen_url

    signals: List[Dict] = [{
        "source": intent_source,
        "description": desc[:499],
        "url": chosen_url,
        "date": sig_date,
        "snippet": str(primary_snippet),
    }]

    seen_urls = {chosen_url}
    if evidence:
        for ev in ranked_ev:
            ev_url = ev.get("url", "")
            if (ev_url and ev_url.startswith(("http://", "https://"))
                    and ev_url not in seen_urls
                    and ev.get("extracted_date")
                    and ev.get("extracted_text")
                    and len(ev.get("extracted_text", "")) >= 30):
                ev_src = _classify_source(ev_url)
                if ev_src not in INTENT_SOURCE_ENUM:
                    ev_src = "company_website"
                _raw_ev = (ev["extracted_text"] or "")
                ev_desc = _raw_ev[:499]
                ev_snippet = _raw_ev[:999]
                if not ev_snippet or len(ev_snippet.strip()) < 10:
                    ev_snippet = ev_desc[:999] if ev_desc else ev_url
                signals.append({
                    "source": ev_src,
                    "description": ev_desc,
                    "url": ev_url,
                    "date": ev["extracted_date"],
                    "snippet": str(ev_snippet),
                })
                seen_urls.add(ev_url)
            if len(signals) >= 5:
                break

    for sig in signals:
        if not sig.get("snippet") or not isinstance(sig["snippet"], str):
            sig["snippet"] = sig.get("description", sig.get("url", ""))[:999]

    return {
        "lead_id": int(lead_id),
        "business": business,
        "company_linkedin": linkedin or "",
        "company_website": website or "",
        "employee_count": _s(lead.get("employee_count")),
        "industry": lead_industry,
        "sub_industry": lead_sub,
        "country": country,
        "city": _s(lead.get("city")),
        "state": _s(lead.get("state")),
        "role": role,
        "role_type": _s(lead.get("role_type")),
        "seniority": seniority,
        "intent_signals": signals,
    }

def _precheck_output(output: Dict, ctx: Dict) -> bool:
    if set(output.keys()) != OUTPUT_FIELDS:
        return False

    if ctx["industry"] and output["industry"]:
        if _fuzz.ratio(_sl(output["industry"]),
                       _sl(ctx["industry"])) < 80:
            return False
    if ctx["sub_industry"] and output["sub_industry"]:
        if _fuzz.ratio(_sl(output["sub_industry"]),
                       _sl(ctx["sub_industry"])) < 70:
            return False
    if ctx["target_roles"] and output["role"]:
        best = max((_fuzz.partial_ratio(_sl(output["role"]), _sl(r))
                    for r in ctx["target_roles"] if r), default=0)
        if best < 60:
            return False

    for field_name in ("business", "role"):
        val = _s(output.get(field_name))
        vl = val.lower()
        if vl in ("test", "asdf", "xxx", "sample", "example", "lorem",
                   "ipsum", "foo", "bar", "baz", "qwerty", "dummy",
                   "fake", "placeholder", "demo", "temp", "null",
                   "undefined", "n/a", "na", "none", "tbd", "todo",
                   "fixme", "testing", "aaa", "bbb", "ccc"):
            return False
        if _SUSPICIOUS_CHARS.search(val):
            return False
        if field_name == "business" and val.strip().isdigit():
            return False
        if len(val) >= 5:
            for ch in set(vl):
                if vl.count(ch) / len(vl) > 0.7:
                    return False
        if not val:
            return False

    biz = _s(output.get("business"))
    for pat in _FAKE_COMPANY:
        if pat.match(biz):
            return False
    role = _s(output.get("role"))
    for pat in _FAKE_ROLE:
        if pat.match(role):
            return False

    if output["seniority"] not in SENIORITY_ENUM:
        return False

    signals = output.get("intent_signals", [])
    if not signals or not isinstance(signals, list):
        return False
    for sig in signals:
        if not sig.get("description") or not sig.get("url"):
            return False
        if sig["source"] not in INTENT_SOURCE_ENUM:
            return False
        if not sig["url"].startswith(("http://", "https://")):
            return False
        if len(sig["description"]) > 500:
            return False
        if not sig.get("snippet"):
            return False
        if len(sig["snippet"]) > 1000:
            return False
        sig_date = sig.get("date", "")
        if not sig_date:
            return False
        try:
            datetime.strptime(sig_date, "%Y-%m-%d")
        except (ValueError, TypeError):
            return False

    icp_country = _s(ctx.get("country", ""))
    if icp_country:
        lead_country = _s(output.get("country", ""))
        if not lead_country or lead_country.strip().lower() != icp_country.strip().lower():
            return False

    icp_sen = _s(ctx.get("target_seniority", ""))
    if icp_sen and icp_sen in _PRECHECK_SENIORITY:
        out_sen = _s(output.get("seniority", ""))
        if out_sen in _PRECHECK_SENIORITY:
            out_idx = _PRECHECK_SENIORITY.index(out_sen)
            icp_idx = _PRECHECK_SENIORITY.index(icp_sen)
            if out_idx > icp_idx + 1:
                return False

    for sig in signals:
        if sig["source"] == "other" and len(sig.get("description", "")) < 100:
            return False

    dl = signals[0]["description"].lower().strip()
    for rx in _GENERIC_INTENT_RX:
        if rx.search(dl):
            return False
    has_kw = any(kw in dl for kw in SPECIFIC_INTENT_KW)
    if len(dl) < 80 and not has_kw:
        return False
    if _TEMPLATED_RX.match(dl) and not has_kw:
        return False

    return True

def _url_score(ev: Dict, icp_kw: Optional[Set[str]] = None) -> float:
    text = (ev.get("extracted_text") or "").lower()
    anchor_count = sum(1 for a in EVIDENCE_ANCHORS if a in text)
    anchor_score = min(1.0, anchor_count / 3.0)
    icp_keyword_hit = 1.0 if (icp_kw and any(kw in text for kw in icp_kw)) else 0.0
    sig_date = ev.get("extracted_date", "")
    recency_score = 0.25
    if sig_date:
        try:
            d = date.fromisoformat(sig_date)
            age_months = (date.today() - d).days / 30.0
            recency_score = 1.0 if age_months <= 2 else (0.5 if age_months <= 12 else 0.25)
        except Exception:
            pass
    return 0.45 * anchor_score + 0.35 * icp_keyword_hit + 0.20 * recency_score

def _select_best_evidence_for_llm(
    evidence: List[Dict],
    icp_kw: Optional[Set[str]] = None,
    max_items: int = 2,
    icp_intent_cat: str = "general",
    icp_action_kw: Optional[List[str]] = None,
    ctx: Optional[Dict] = None,
    company: str = "",
) -> List[Dict]:
    if not evidence:
        return []
    clean = [e for e in evidence
             if e.get("_date_status") != "fabricated"
             and e.get("_quality_score", 0) > 0
             and not _is_generic_description(e.get("extracted_text", ""))]
    if not clean:
        clean = [e for e in evidence if e.get("_quality_score", 0) > 0]
    if not clean:
        clean = evidence
    if any(e.get("_quality_score") for e in clean):
        ranked = sorted(clean, key=lambda e: -e.get("_quality_score", 0))
    elif icp_action_kw and ctx:
        ranked = _filter_and_rank_evidence(
            clean, ctx, icp_intent_cat, icp_action_kw, company,
        )
    else:
        ranked = clean
    if not ranked:
        ranked = sorted(clean, key=lambda ev: -_url_score(ev, icp_kw))
    seen_sources: Set[str] = set()
    diverse: List[Dict] = []
    for ev in ranked:
        src = ev.get("source", "other")
        if src not in seen_sources or len(diverse) < max_items:
            diverse.append(ev)
            seen_sources.add(src)
        if len(diverse) >= max_items:
            break
    return diverse if diverse else ranked[:max_items]

def _intent_pre_score(
    evidence: List[Dict],
    icp_kw: Optional[Set[str]] = None,
    icp_intent_cat: str = "general",
    icp_action_kw: Optional[List[str]] = None,
    company: str = "",
) -> float:
    if not evidence:
        return 0.0
    best = -1.0
    for ev in evidence:
        u = _url_score(ev, icp_kw)
        src = ev.get("source", "other")
        mult = SOURCE_MULTIPLIERS.get(src, 0.3)
        base = u * mult
        if icp_action_kw:
            rel = _evidence_icp_relevance(ev, icp_intent_cat, icp_action_kw or [], company)
            base = base * 0.55 + rel * 0.45
        best = max(best, base)
    return best

_RF_MATCH_KEYS = [
    "geo", "industry", "sub_industry", "role", "emp",
    "business", "description", "seniority",
    "role_type_dm", "decision_maker", "domain_overlap",
]
_RF_INTENT_KEYS = [
    "linkedin_present", "job_board_present", "github_present",
    "company_website_present", "news_present", "wikipedia_present",
    "recency_score", "source_multiplier_weighted_sum", "total_sources",
]

def _build_rf_feature_vector(
    match_feats: Dict[str, float],
    intent_feats: Dict[str, float],
    intent_pre: float,
) -> List[float]:
    vec: List[float] = []
    for k in _RF_MATCH_KEYS:
        vec.append(float(match_feats.get(k, 0.5)))
    for k in _RF_INTENT_KEYS:
        vec.append(float(intent_feats.get(k, 0.0)))
    vec.append(intent_pre)
    return vec

def _random_forest_predict(
    X: List[List[float]],
    trees: List[Any],
    rng: random.Random,
) -> List[float]:
    if not trees or not X:
        return [0.0] * len(X)
    preds = []
    for xi in X:
        tree_preds = []
        for tree in trees:
            node = tree
            while "split_feat" in node:
                feat_idx = node["split_feat"]
                thresh = node["split_thresh"]
                if xi[feat_idx] <= thresh:
                    node = node["left"]
                else:
                    node = node["right"]
            tree_preds.append(node["value"])
        preds.append(sum(tree_preds) / len(tree_preds))
    return preds

def _fit_simple_tree(
    X: List[List[float]],
    y: List[float],
    feat_indices: List[int],
    max_depth: int,
    rng: random.Random,
) -> Dict[str, Any]:
    if max_depth <= 0 or len(y) < 2 or not feat_indices:
        return {"value": sum(y) / len(y) if y else 0.0}
    best_mse = float("inf")
    best_feat, best_thresh, best_left, best_right = None, None, [], []
    for _ in range(min(3, len(feat_indices))):
        fi = rng.choice(feat_indices)
        vals = [row[fi] for row in X]
        if min(vals) >= max(vals):
            continue
        for _ in range(5):
            thresh = rng.uniform(min(vals), max(vals))
            left_idx = [i for i in range(len(X)) if X[i][fi] <= thresh]
            right_idx = [i for i in range(len(X)) if X[i][fi] > thresh]
            if not left_idx or not right_idx:
                continue
            left_y = [y[i] for i in left_idx]
            right_y = [y[i] for i in right_idx]
            m_left = sum(left_y) / len(left_y)
            m_right = sum(right_y) / len(right_y)
            mse = sum((v - m_left) ** 2 for v in left_y) + sum((v - m_right) ** 2 for v in right_y)
            if mse < best_mse:
                best_mse = mse
                best_feat = fi
                best_thresh = thresh
                best_left = left_idx
                best_right = right_idx
    if best_feat is None:
        return {"value": sum(y) / len(y) if y else 0.0}
    left_X = [X[i] for i in best_left]
    left_y = [y[i] for i in best_left]
    right_X = [X[i] for i in best_right]
    right_y = [y[i] for i in best_right]
    return {
        "split_feat": best_feat,
        "split_thresh": best_thresh,
        "left": _fit_simple_tree(left_X, left_y, feat_indices, max_depth - 1, rng),
        "right": _fit_simple_tree(right_X, right_y, feat_indices, max_depth - 1, rng),
        "value": sum(y) / len(y),
    }

def _train_random_forest(
    X: List[List[float]],
    y: List[float],
    n_estimators: int = 10,
    max_depth: int = 4,
    n_features: Optional[int] = None,
    seed: int = 42,
) -> List[Dict[str, Any]]:
    if not X or not y or len(X) != len(y):
        return []
    n = len(X)
    n_feat = len(X[0])
    n_feat_sample = n_features or max(1, int(n_feat ** 0.5))
    rng = random.Random(seed)
    trees = []
    for _ in range(n_estimators):
        idx = [rng.randint(0, n - 1) for _ in range(n)]
        X_boot = [X[i] for i in idx]
        y_boot = [y[i] for i in idx]
        feat_idx = rng.sample(range(n_feat), min(n_feat_sample, n_feat))
        tree = _fit_simple_tree(X_boot, y_boot, feat_idx, max_depth, rng)
        trees.append(tree)
    return trees

def _rank_by_random_forest(
    candidates_with_evidence: List[Tuple[Dict, float, Dict, List[Dict], Dict]],
    learner: Any,
    icp_kw: Optional[Set[str]] = None,
) -> List[Tuple[Dict, float, Dict, List[Dict], Dict]]:
    if not candidates_with_evidence:
        return candidates_with_evidence

    match_log = getattr(learner, "_match_log", [])
    intent_log = getattr(learner, "_intent_log", [])
    min_train = 5
    if len(match_log) < min_train or len(intent_log) < min_train:
        return sorted(
            candidates_with_evidence,
            key=lambda x: -_intent_pre_score(x[3], icp_kw),
        )

    X_train = []
    y_train = []
    for i in range(min(len(match_log), len(intent_log))):
        _, mf, outcome = match_log[i]
        itf, _ = intent_log[i]
        intent_pre = 0.0
        for k, v in itf.items():
            if "present" in k and v > 0:
                intent_pre += 0.1
        intent_pre = min(1.0, intent_pre + itf.get("recency_score", 0) * 0.5)
        vec = _build_rf_feature_vector(mf, itf, intent_pre)
        X_train.append(vec)
        y_train.append(outcome)

    trees = _train_random_forest(X_train, y_train, n_estimators=10, max_depth=4)
    if not trees:
        return sorted(
            candidates_with_evidence,
            key=lambda x: -_intent_pre_score(x[3], icp_kw),
        )

    X_pred = []
    for item in candidates_with_evidence:
        _, _, mf, evidence, itf = item
        intent_pre = _intent_pre_score(evidence, icp_kw)
        vec = _build_rf_feature_vector(mf, itf, intent_pre)
        X_pred.append(vec)

    preds = _random_forest_predict(X_pred, trees, random.Random(42))
    scored = list(zip(candidates_with_evidence, preds))
    scored.sort(key=lambda x: -x[1])
    return [item for item, _ in scored]

def _llm_pick_winner_from_evidence(
    candidates_with_evidence: List[Tuple[Dict, float, Dict, List[Dict], Dict]],
    ctx: Dict,
    proxy_url: str,
) -> List[Tuple[Dict, float, Dict, List[Dict], Dict]]:
    if not candidates_with_evidence or not proxy_url:
        return candidates_with_evidence
    if len(candidates_with_evidence) <= 1:
        return candidates_with_evidence

    icp_text = _build_icp_text(ctx)
    candidate_lines: List[str] = []
    for idx, (lead, rr_score, _, evidence, _) in enumerate(candidates_with_evidence):
        company = _s(lead.get("business", ""))
        industry = _s(lead.get("industry", ""))
        country = _s(lead.get("country", ""))
        ev_summaries = []
        for ev in evidence[:2]:
            src = ev.get("source", "")
            txt = (ev.get("extracted_text") or "")[:200]
            dt = ev.get("extracted_date", "")
            ev_summaries.append(f"  [{src}] {txt} (date: {dt})")
        ev_block = "\n".join(ev_summaries) if ev_summaries else "  (no evidence)"
        candidate_lines.append(
            f"CANDIDATE {idx}: {company} | {industry} | {country}\n"
            f"  rerank_score: {rr_score:.3f}\n{ev_block}")

    prompt = (
        "You are a B2B lead qualification expert. Given an ICP and "
        "candidate leads with their evidence, rank ALL candidates "
        "from BEST to WORST match for the ICP.\n\n"
        f"ICP:\n{icp_text}\n\n"
        f"CANDIDATES:\n" + "\n\n".join(candidate_lines) + "\n\n"
        "TASK: Rank candidates from best to worst ICP match.\n"
        "Consider: industry fit, geography match, evidence strength "
        "(hiring, funding, expansion signals), and recency.\n\n"
        "Return ONLY a JSON array of candidate indices in order from "
        "best to worst, e.g. [2, 0, 4, 1, 3]. No markdown."
    )
    resp = _openrouter_chat(prompt, proxy_url)
    if not resp:
        return candidates_with_evidence
    try:
        cleaned = resp.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)
        ranked_indices = json.loads(cleaned)
        if not isinstance(ranked_indices, list):
            return candidates_with_evidence
        n = len(candidates_with_evidence)
        valid = [i for i in ranked_indices if isinstance(i, int) and 0 <= i < n]
        seen: Set[int] = set()
        unique: List[int] = []
        for i in valid:
            if i not in seen:
                unique.append(i)
                seen.add(i)
        for i in range(n):
            if i not in seen:
                unique.append(i)
                seen.add(i)
        result = [candidates_with_evidence[i] for i in unique]
        _debug_log("LLM winner selection reordered %d candidates: %s",
                   n, unique[:5])
        return result
    except Exception:
        return candidates_with_evidence


_BANDIT_SOURCES: List[str] = [
    "linkedin", "company_website", "news", "linkedin_jobs",
    "google_jobs", "github", "duckduckgo", "wikipedia",
]

def _is_strong_evidence(ev: Dict, icp_kw: Optional[Set[str]] = None) -> bool:
    has_date = bool(ev.get("extracted_date"))
    if not has_date:
        return False
    text = (ev.get("extracted_text") or "").lower()
    if not any(kw in text for kw in SPECIFIC_INTENT_KW):
        return False
    if icp_kw and any(kw in text for kw in icp_kw):
        return True
    return True

class SourceBandit:
    def __init__(self):
        self._stats: Dict[str, Dict[str, Any]] = {}
        for s in _BANDIT_SOURCES:
            self._stats[s] = {"trials": 0, "successes": 0, "total_quality": 0.0}

    def select_next(self, exclude: Set[str]) -> Optional[str]:
        t = sum(self._stats[s]["trials"] for s in _BANDIT_SOURCES)
        if t == 0:
            for s in _BANDIT_SOURCES:
                if s not in exclude:
                    return s
            return None
        best_s, best_u = None, -1e9
        for s in _BANDIT_SOURCES:
            if s in exclude:
                continue
            st = self._stats[s]
            n = st["trials"]
            if n == 0:
                return s
            avg = st["total_quality"] / n if n else 0.0
            ucb = avg + math.sqrt(2.0 * math.log(t + 1) / (n + 1e-9))
            if ucb > best_u:
                best_u = ucb
                best_s = s
        return best_s

    def update(self, source: str, ev: Optional[Dict], icp_kw: Optional[Set[str]] = None) -> None:
        if source not in self._stats:
            return
        st = self._stats[source]
        st["trials"] += 1
        if ev and _is_strong_evidence(ev, icp_kw):
            st["successes"] += 1
            q = _url_score(ev, icp_kw)
            st["total_quality"] += q

_source_bandit = SourceBandit()

_DEFAULT_MATCH_WEIGHTS: Dict[str, float] = {
    "w_geo": 0.19, "w_industry": 0.17, "w_sub_industry": 0.16,
    "w_role": 0.15, "w_emp": 0.08, "w_business": 0.07,
    "w_description": 0.07, "w_seniority": 0.05,
    "w_role_type_dm": 0.03, "w_decision_maker": 0.015,
    "w_domain_overlap": 0.005,
}

_DEFAULT_SOURCE_PRIORITY: List[str] = [
    "linkedin", "job_board", "news", "github",
    "company_website", "social_media",
]

def _extract_icp_summary(ctx: Dict) -> Dict[str, str]:
    return {
        "industry": _sl(ctx.get("industry", "")),
        "sub_industry": _sl(ctx.get("sub_industry", "")),
        "target_seniority": _sl(ctx.get("target_seniority", "")),
        "country": _sl(ctx.get("country", "")),
        "state": _sl(ctx.get("state", "")),
        "city": _sl(ctx.get("city", "")),
    }

def _icp_similarity(curr: Dict, past: Dict) -> float:
    if not past:
        return 1.0
    sim = 0.0
    w = 0.0
    fields = ("industry", "sub_industry", "target_seniority",
              "country", "state", "city")
    per = 1.0 / len(fields)
    for k in fields:
        c, p = _sl(curr.get(k, "")), _sl(past.get(k, ""))
        if c or p:
            w += per
            if c and p and (c == p or c in p or p in c):
                sim += per
    return (sim / w) if w > 0 else 0.5

class AdaptiveLearner:

    _MIN_SAMPLES = 3
    _MAX_LOG = 60
    _PI_ITERS = 5
    _MIN_ICP_SIM = 0.2

    def __init__(self):
        self._match_log: List[Tuple[Optional[Dict], Dict[str, float], float]] = []
        self._intent_log: List[Tuple[Dict[str, float], float]] = []
        self._source_outcomes: Dict[str, List[float]] = {}
        self._none_count: int = 0
        self._cached_weights: Optional[Dict[str, float]] = None
        self._cached_priority: Optional[List[str]] = None
        self._cached_importances: Optional[Dict[str, float]] = None
        self._run_count: int = 0
        self._scored_count: int = 0

    def log_run(self, match_features: Dict[str, float],
                intent_features: Dict[str, float],
                outcome_score: float,
                icp_context: Optional[Dict] = None):
        icp_sum = _extract_icp_summary(icp_context) if icp_context else None
        self._match_log.append((icp_sum, dict(match_features), outcome_score))
        self._intent_log.append((dict(intent_features), outcome_score))

        for sk in _DEFAULT_SOURCE_PRIORITY:
            if intent_features.get(f"{sk}_present", 0) > 0:
                self._source_outcomes.setdefault(sk, []).append(
                    outcome_score)

        if len(self._match_log) > self._MAX_LOG:
            self._match_log = self._match_log[-self._MAX_LOG:]
            self._intent_log = self._intent_log[-self._MAX_LOG:]

        self._scored_count += 1
        self._run_count += 1

        if self._scored_count >= self._MIN_SAMPLES:
            self._recompute()

    def log_none(self):
        self._none_count += 1
        self._run_count += 1

    def get_reranker_weights(self, icp_ctx: Optional[Dict] = None) -> Optional[Dict[str, float]]:
        if not self._match_log or len(self._match_log) < self._MIN_SAMPLES:
            return self._cached_weights
        if icp_ctx:
            return self._recompute_for_icp(_extract_icp_summary(icp_ctx))
        return self._cached_weights

    def get_source_priority(self) -> Optional[List[str]]:
        return self._cached_priority

    def get_importances(self) -> Optional[Dict[str, float]]:
        return self._cached_importances

    @property
    def run_count(self) -> int:
        return self._run_count

    @property
    def scored_count(self) -> int:
        return self._scored_count

    def _recompute(self):
        n = len(self._match_log)
        if n < self._MIN_SAMPLES:
            return

        feat_names = list(self._match_log[0][1].keys())
        scores = [entry[2] for entry in self._match_log]
        mean_score = sum(scores) / n

        pi_importances: Dict[str, float] = {}
        for feat in feat_names:
            drops: List[float] = []
            for _ in range(self._PI_ITERS):
                vals = [entry[1].get(feat, 0.0) for entry in self._match_log]
                random.shuffle(vals)
                corrupted_scores: List[float] = []
                for i, (_icp, mf, orig_score) in enumerate(self._match_log):
                    modified = dict(mf)
                    modified[feat] = vals[i]
                    proxy = (0.18 * modified.get("geo", 0)
                             + 0.17 * modified.get("industry", 0)
                             + 0.16 * modified.get("sub_industry", 0)
                             + 0.15 * modified.get("role", 0)
                             + 0.08 * modified.get("emp", 0)
                             + 0.08 * modified.get("business", 0)
                             + 0.08 * modified.get("description", 0)
                             + 0.05 * modified.get("seniority", 0)
                             + 0.03 * modified.get("role_type_dm", 0)
                             + 0.015 * modified.get("decision_maker", 0)
                             + 0.005 * modified.get("domain_overlap", 0))
                    corrupted_scores.append(
                        proxy * (orig_score / max(mean_score, 0.01)))
                shuffled_mean = (sum(corrupted_scores) / n
                                 if corrupted_scores else 0)
                original_proxy_mean = (
                    sum(entry[2] for entry in self._match_log) / n)
                drops.append(original_proxy_mean - shuffled_mean)
            pi_importances[feat] = (
                max(0.0, sum(drops) / len(drops)) if drops else 0.0)

        pi_total = sum(pi_importances.values())
        if pi_total > 0:
            pi_norm = {k: v / pi_total for k, v in pi_importances.items()}
        else:
            pi_norm = {k: 1.0 / len(feat_names) for k in feat_names}

        corr_scores: Dict[str, float] = {}
        std_score = math.sqrt(
            sum((s - mean_score) ** 2 for s in scores) / n)
        for feat in feat_names:
            vals = [entry[1].get(feat, 0.0) for entry in self._match_log]
            mean_feat = sum(vals) / n
            std_feat = math.sqrt(
                sum((v - mean_feat) ** 2 for v in vals) / n)
            if std_feat > 1e-9 and std_score > 1e-9:
                cov = sum((v - mean_feat) * (s - mean_score)
                          for v, s in zip(vals, scores)) / n
                corr = cov / (std_feat * std_score)
            else:
                corr = 0.0
            corr_scores[feat] = max(0.0, corr)

        corr_total = sum(corr_scores.values())
        if corr_total > 0:
            corr_norm = {k: v / corr_total for k, v in corr_scores.items()}
        else:
            corr_norm = {k: 1.0 / len(feat_names) for k in feat_names}

        combined: Dict[str, float] = {}
        for feat in feat_names:
            combined[feat] = 0.5 * pi_norm.get(feat, 0) + \
                             0.5 * corr_norm.get(feat, 0)
        comb_total = sum(combined.values())
        if comb_total > 0:
            combined = {k: v / comb_total for k, v in combined.items()}

        self._cached_importances = dict(
            sorted(combined.items(), key=lambda x: -x[1]))

        alpha = min(0.5, self._scored_count / 20.0)

        learned_weights: Dict[str, float] = {}
        for feat in feat_names:
            wk = f"w_{feat}"
            default_val = _DEFAULT_MATCH_WEIGHTS.get(wk, 1.0 / 6.0)
            learned_val = combined.get(feat, default_val)
            learned_weights[wk] = ((1 - alpha) * default_val
                                   + alpha * learned_val)

        w_total = sum(learned_weights.values())
        if w_total > 0:
            learned_weights = {k: v / w_total
                               for k, v in learned_weights.items()}
        self._cached_weights = learned_weights

        source_avg: Dict[str, float] = {}
        for sk in _DEFAULT_SOURCE_PRIORITY:
            outcomes = self._source_outcomes.get(sk, [])
            if outcomes:
                source_avg[sk] = sum(outcomes) / len(outcomes)
            else:
                source_avg[sk] = mean_score * 0.5

        self._cached_priority = sorted(
            _DEFAULT_SOURCE_PRIORITY,
            key=lambda s: -source_avg.get(s, 0.0))

    def _recompute_for_icp(self, curr_icp_summary: Dict) -> Optional[Dict[str, float]]:
        n = len(self._match_log)
        if n < self._MIN_SAMPLES:
            return self._cached_weights

        run_weights = [
            max(self._MIN_ICP_SIM, _icp_similarity(curr_icp_summary, entry[0] or {}))
            for entry in self._match_log
        ]
        w_sum = sum(run_weights)
        if w_sum < 1e-9:
            return self._cached_weights
        norm_weights = [w / w_sum for w in run_weights]

        feat_names = list(self._match_log[0][1].keys())
        scores = [entry[2] for entry in self._match_log]
        mean_score = sum(s * nw for s, nw in zip(scores, norm_weights))

        pi_importances: Dict[str, float] = {}
        for feat in feat_names:
            drops: List[float] = []
            for _ in range(self._PI_ITERS):
                vals = [entry[1].get(feat, 0.0) for entry in self._match_log]
                random.shuffle(vals)
                corrupted_scores: List[float] = []
                for i, (_icp, mf, orig_score) in enumerate(self._match_log):
                    modified = dict(mf)
                    modified[feat] = vals[i]
                    proxy = sum(
                        _DEFAULT_MATCH_WEIGHTS.get(f"w_{f}", 0.05) * modified.get(f, 0)
                        for f in modified)
                    corrupted_scores.append(
                        norm_weights[i] * proxy * (orig_score / max(mean_score, 0.01)))
                orig_proxies = []
                for j, (_icp, mf, sc) in enumerate(self._match_log):
                    p = sum(
                        _DEFAULT_MATCH_WEIGHTS.get(f"w_{f}", 0.05) * mf.get(f, 0)
                        for f in mf)
                    orig_proxies.append(norm_weights[j] * p * (sc / max(mean_score, 0.01)))
                drops.append(sum(orig_proxies) - sum(corrupted_scores))
            pi_importances[feat] = max(0.0, sum(drops) / len(drops)) if drops else 0.0

        pi_total = sum(pi_importances.values())
        pi_norm = {k: v / pi_total for k, v in pi_importances.items()} if pi_total > 0 else {k: 1.0 / len(feat_names) for k in feat_names}

        std_score = math.sqrt(sum(nw * (s - mean_score) ** 2 for s, nw in zip(scores, norm_weights)))
        corr_scores: Dict[str, float] = {}
        for feat in feat_names:
            vals = [entry[1].get(feat, 0.0) for entry in self._match_log]
            mean_feat = sum(v * nw for v, nw in zip(vals, norm_weights))
            std_feat = math.sqrt(sum(nw * (v - mean_feat) ** 2 for v, nw in zip(vals, norm_weights)))
            if std_feat > 1e-9 and std_score > 1e-9:
                cov = sum(nw * (v - mean_feat) * (s - mean_score) for v, s, nw in zip(vals, scores, norm_weights))
                corr = cov / (std_feat * std_score)
            else:
                corr = 0.0
            corr_scores[feat] = max(0.0, corr)
        corr_total = sum(corr_scores.values())
        corr_norm = {k: v / corr_total for k, v in corr_scores.items()} if corr_total > 0 else {k: 1.0 / len(feat_names) for k in feat_names}

        combined = {feat: 0.5 * pi_norm.get(feat, 0) + 0.5 * corr_norm.get(feat, 0) for feat in feat_names}
        comb_total = sum(combined.values())
        combined = {k: v / comb_total for k, v in combined.items()} if comb_total > 0 else combined

        alpha = min(0.5, self._scored_count / 20.0)
        learned: Dict[str, float] = {}
        for feat in feat_names:
            wk = f"w_{feat}"
            default_val = _DEFAULT_MATCH_WEIGHTS.get(wk, 1.0 / 10.0)
            learned[wk] = (1 - alpha) * default_val + alpha * combined.get(feat, default_val)
        w_total = sum(learned.values())
        return {k: v / w_total for k, v in learned.items()} if w_total > 0 else self._cached_weights

_learner = AdaptiveLearner()

_seen_lead_ids: Set = set()
_seen_companies: Set[str] = set()

def find_leads(icp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    global _page_cache, _desearch_date_cache, _last_modified_cache, _run_proxy_url, _llm_icp_kw_cache, _llm_intent_cat_cache, _llm_query_cache, _seen_lead_ids, _seen_companies
    _page_cache = {}
    _desearch_date_cache = {}
    _last_modified_cache = {}
    _llm_query_cache = {}
    _llm_icp_kw_cache = {}
    _llm_intent_cat_cache = {}
    t0 = time.time()

    try:
        return _find_leads_inner(icp, t0)
    except Exception:
        traceback.print_exc()
        return None


def _find_leads_inner(icp: Dict[str, Any], t0: float) -> Optional[Dict[str, Any]]:
    global _run_proxy_url
    config = icp.get("_config", {})
    supabase_url = config.get("SUPABASE_URL")
    supabase_key = config.get("SUPABASE_ANON_KEY")
    table = config.get("QUALIFICATION_LEADS_TABLE") or "test_leads_for_miners"
    proxy_url = config.get("PROXY_URL", "http://localhost:8001")
    _run_proxy_url = proxy_url

    if not supabase_url or not supabase_key:
        print(f"[SENOO] EXIT: no supabase creds. url={bool(supabase_url)} key={bool(supabase_key)}", flush=True)
        _learner.log_none()
        return None

    try:
        from supabase import create_client
        db = create_client(supabase_url, supabase_key)
    except Exception as e:
        print(f"[SENOO] EXIT: supabase create_client failed: {e}", flush=True)
        _learner.log_none()
        return None

    ctx = _parse_icp(icp)
    print(f"[SENOO] ICP parsed: industry={ctx.get('industry')}, sub={ctx.get('sub_industry')}, "
          f"country={ctx.get('country')}, state={ctx.get('state')}, city={ctx.get('city')}, "
          f"roles={ctx.get('target_roles')}, seniority={ctx.get('target_seniority')}", flush=True)

    geography = _s(icp.get("geography"))
    if geography and (not ctx["country"] or not ctx["city"]):
        llm_country, llm_state, llm_city = _extract_geography_via_llm(geography, proxy_url)
        if llm_country and not ctx["country"]:
            ctx["country"] = llm_country
        if llm_state and not ctx["state"]:
            ctx["state"] = llm_state
        if llm_city and not ctx["city"]:
            ctx["city"] = llm_city
        print(f"[SENOO] geography LLM: geo='{geography}' -> country='{ctx['country']}', "
              f"state='{ctx.get('state','')}', city='{ctx.get('city','')}'", flush=True)

    candidates = _query_db(db, table, ctx)
    print(f"[SENOO] DB candidates: {len(candidates)}, table={table}", flush=True)
    candidates = [c for c in candidates
                  if (c.get("id") or c.get("lead_id")) not in _seen_lead_ids
                  and _sl(c.get("business", "")).strip() not in _seen_companies]
    print(f"[SENOO] after dedup filter: {len(candidates)}, seen_ids={len(_seen_lead_ids)}, seen_co={len(_seen_companies)}", flush=True)
    if not candidates:
        print("[SENOO] EXIT: no candidates after DB query + dedup", flush=True)
        _learner.log_none()
        return None

    adaptive_match_weights = _learner.get_reranker_weights(icp_ctx=ctx)
    adaptive_source_priority = _learner.get_source_priority()

    if adaptive_match_weights:
        _debug_log("Adaptive reranker weights (run %d): %s",
                   _learner.run_count, adaptive_match_weights)
    if adaptive_source_priority:
        _debug_log("Adaptive source priority (run %d): %s",
                   _learner.run_count, adaptive_source_priority)

    top2 = _hybrid_rerank_top2(candidates, ctx, proxy_url,
                                match_weights=adaptive_match_weights)
    print(f"[SENOO] hybrid rerank: {len(top2)} candidates", flush=True)
    if not top2:
        print("[SENOO] EXIT: no candidates after hybrid rerank", flush=True)
        _learner.log_none()
        return None

    icp_city = _sl(ctx.get("city", ""))
    icp_state = _sl(ctx.get("state", ""))
    if icp_city or icp_state:
        def _geo_tier(lead_state_city):
            lead, _sc = lead_state_city
            ls = _sl(lead.get("state", ""))
            lc = _sl(lead.get("city", ""))
            if icp_city and lc and (lc == icp_city or _fuzz.ratio(lc, icp_city) >= 80):
                if icp_state and ls and _fuzz.ratio(ls, icp_state) >= 80:
                    return 0
                return 1
            if icp_state and ls and _fuzz.ratio(ls, icp_state) >= 80:
                return 1
            return 2
        top2.sort(key=_geo_tier)

    icp_prompt = _s(icp.get("prompt") or icp.get("buyer_description") or "")
    if not icp_prompt:
        icp_prompt = _build_icp_text(ctx)
    buyer_desc = _s(icp.get("buyer_description") or
                    icp.get("product_service") or "")

    best_output: Optional[Dict] = None
    best_score = -1.0
    best_verified_total = -1.0
    best_intent_features: Optional[Dict[str, float]] = None
    best_match_features: Optional[Dict[str, float]] = None
    _fallback: Dict[str, Any] = {"output": None, "score": -1.0, "intent_feats": None, "match_feats": None}
    _TIEBREAK_PRIORITY = ["linkedin", "job_board", "github",
                          "news", "company_website", "wikipedia"]
    icp_kw = _extract_icp_keywords_llm(ctx, proxy_url)

    icp_intent_cat, icp_action_kw = _detect_icp_intent_category_simple(ctx)
    _debug_log("ICP intent category: %s  action_kw: %s", icp_intent_cat, icp_action_kw[:4])

    candidates_with_evidence: List[Tuple[Dict, float, Dict, List[Dict], Dict]] = []
    _bare_candidates: List[Tuple[Dict, float, Dict]] = []
    for idx, (lead, rerank_score) in enumerate(top2[:7]):
        if time.time() - t0 > _PIPELINE_BUDGET - 1.5:
            break
        company = _s(lead.get("business"))
        if not company:
            continue
        try:
            match_feats = _extract_match_features(lead, ctx)
            _bare_candidates.append((lead, rerank_score, match_feats))
            evidence = _discover_intent_sources(
                lead, company, proxy_url, t0,
                source_priority=adaptive_source_priority,
                icp_intent_signals=ctx.get("intent_signals"),
                icp_kw=icp_kw,
                icp_text=_build_icp_text(ctx),
                icp_target_roles=ctx.get("target_roles"),
                icp_ctx=ctx)
            if not evidence:
                continue
            evidence = _filter_and_rank_evidence(
                evidence, ctx, icp_intent_cat, icp_action_kw, company)
            intent_feats = _compute_intent_features(evidence)
            candidates_with_evidence.append((lead, rerank_score, match_feats, evidence, intent_feats))
        except Exception as e:
            print(f"[SENOO] candidate {company} failed: {e}", flush=True)
            continue

    print(f"[SENOO] intent discovery: {len(candidates_with_evidence)} with evidence, "
          f"{len(_bare_candidates)} bare, budget_left={_PIPELINE_BUDGET - (time.time() - t0):.1f}s", flush=True)

    if len(candidates_with_evidence) > 1:
        def _predicted_total(c):
            _cl, _cr, _cm, _ce, _ci = c
            return (_predict_icp_fit(_cm)
                    + _predict_dm_score(_cl, ctx)
                    + max((e.get("_quality_score", 0) for e in _ce), default=0))
        candidates_with_evidence.sort(key=_predicted_total, reverse=True)
        if candidates_with_evidence:
            _top = candidates_with_evidence[0]
            _pt = _predicted_total(_top)
            print(f"[SENOO] top predicted total={_pt:.1f} for "
                  f"{_s(_top[0].get('business'))}", flush=True)

    candidates_with_evidence = _rank_by_random_forest(
        candidates_with_evidence, _learner, icp_kw
    )

    def _try_llm_verify(lead: Dict, evidence: List[Dict], match_feats: Dict,
                        intent_feats: Dict, rerank_score: float) -> bool:
        nonlocal best_output, best_score, best_verified_total
        nonlocal best_intent_features, best_match_features
        company = _s(lead.get("business"))
        evidence_for_llm = _select_best_evidence_for_llm(
            evidence, icp_kw, max_items=2,
            icp_intent_cat=icp_intent_cat,
            icp_action_kw=icp_action_kw,
            ctx=ctx, company=company,
        )
        if not evidence_for_llm:
            evidence_for_llm = evidence[:2]
        verification = _llm_verify_candidate(
            company, icp_prompt, buyer_desc, evidence_for_llm, proxy_url,
            icp_intent_signals=ctx.get("intent_signals"))
        confidence = verification["confidence"]
        intent_final = verification["intent_final"]
        if confidence == 0:
            return False
        p_icp = _predict_icp_fit(match_feats)
        p_dm = _predict_dm_score(lead, ctx)
        verified_total = p_icp + p_dm + intent_final
        print(f"[SENOO] verified {company}: p_icp={p_icp:.1f} "
              f"p_dm={p_dm:.1f} intent={intent_final:.1f} "
              f"total={verified_total:.1f} conf={confidence}", flush=True)
        if confidence < 10:
            fb_output = _build_output(lead, ctx, verification, evidence=evidence)
            if fb_output and _precheck_output(fb_output, ctx):
                fb_total = p_icp + p_dm + intent_final * 0.5
                if fb_total > _fallback["score"]:
                    _fallback["output"] = fb_output
                    _fallback["score"] = fb_total
                    _fallback["intent_feats"] = intent_feats
                    _fallback["match_feats"] = match_feats
            return False
        output = _build_output(lead, ctx, verification, evidence=evidence)
        if not output or not _precheck_output(output, ctx):
            return False
        if verified_total > best_verified_total:
            best_output = output
            best_score = intent_final
            best_verified_total = verified_total
            best_intent_features = intent_feats
            best_match_features = match_feats
        return verified_total > 75.0

    for i, (lead, rerank_score, match_feats, evidence, intent_feats) in enumerate(candidates_with_evidence):
        if time.time() - t0 > _PIPELINE_BUDGET - 1.5:
            break
        try:
            if _try_llm_verify(lead, evidence, match_feats, intent_feats, rerank_score):
                break
        except Exception:
            continue

    fallback_out = _fallback["output"]
    if best_output is None and fallback_out is not None:
        best_output = fallback_out
        best_score = _fallback["score"]
        best_intent_features = _fallback["intent_feats"]
        best_match_features = _fallback["match_feats"]

    if best_output is None and top2 and time.time() - t0 < _PIPELINE_BUDGET - 1.0:
        intent_kw = ctx.get("intent_signals") or []
        parts = []
        for s in intent_kw[:3]:
            if isinstance(s, str) and s.strip():
                parts.extend(str(s).lower().split()[:5])
        intent_q = " ".join(parts) if parts else "hiring funding expansion launch"
        for lead, _ in top2:
            company = _s(lead.get("business"))
            if not company:
                continue
            web_q = f'"{company}" {intent_q}'
            web_results = _desearch(web_q, proxy_url, timeout=3.0)
            for item in web_results[:5]:
                if time.time() - t0 > _PIPELINE_BUDGET - 0.5:
                    break
                url = _s(item.get("url") or item.get("link"))
                if not url or not url.startswith("http"):
                    continue
                domain = urlparse(url.lower()).netloc
                if "wikipedia.org" in domain:
                    continue
                if any(d in domain for d in BLOCKED_DOMAINS):
                    continue
                html = _scrape(url, proxy_url)
                ev = _extract_evidence_item(
                    url, html, company,
                    _classify_source(url), lenient=True, icp_keywords=icp_kw)
                if ev and ev.get("extracted_date"):
                    last_resort_verification = _llm_verify_candidate(
                        company, icp_prompt, buyer_desc, [ev], proxy_url,
                        icp_intent_signals=ctx.get("intent_signals"))
                    if last_resort_verification.get("confidence", 0) >= 10:
                        fallback_output = _build_output(
                            lead, ctx, last_resort_verification, evidence=[ev])
                        if fallback_output and _precheck_output(fallback_output, ctx):
                            best_output = fallback_output
                            best_score = last_resort_verification.get("intent_final", 0.5)
                            best_match_features = _extract_match_features(lead, ctx)
                            best_intent_features = _compute_intent_features([ev])
                            break
            if best_output:
                break

    _INNER_PAGES = ["/about", "/news"]
    if best_output is None and _bare_candidates and time.time() - t0 < _PIPELINE_BUDGET - 0.5:
        _debug_log("All paths failed — attempting bare-minimum website fallback")
        for bare_lead, _, bare_mfeats in _bare_candidates:
            if time.time() - t0 > _PIPELINE_BUDGET:
                break
            website = _s(bare_lead.get("website") or bare_lead.get("company_website"))
            linkedin_url = _s(bare_lead.get("company_linkedin"))
            if not website and not linkedin_url:
                continue
            if website and not website.startswith(("http://", "https://")):
                website = "https://" + website
            if linkedin_url and not linkedin_url.startswith(("http://", "https://")):
                linkedin_url = "https://" + linkedin_url
            company_name = _s(bare_lead.get("business"))

            urls_to_try = []
            if website:
                urls_to_try.append(website)
                base = website.rstrip("/")
                for suffix in _INNER_PAGES:
                    urls_to_try.append(base + suffix)

            ev = None
            ev_date = ""
            ev_desc = ""
            chosen_url = ""
            for try_url in urls_to_try:
                if time.time() - t0 > _PIPELINE_BUDGET:
                    break
                html = _scrape(try_url, proxy_url)
                if not html or len(html) < 200:
                    continue
                candidate_ev = _extract_evidence_item(
                    try_url, html, company_name,
                    "company_website", lenient=True,
                    icp_keywords=icp_kw)
                if not candidate_ev:
                    continue
                ev = candidate_ev
                ev_desc = ev.get("extracted_text", "")
                ev_date = ev.get("extracted_date", "")
                chosen_url = try_url
                if ev_date:
                    break

            if not ev and linkedin_url and time.time() - t0 < _PIPELINE_BUDGET:
                raw = _fetch_linkedin_company(linkedin_url, proxy_url)
                if raw and len(raw) > 60:
                    text_clean = _html_to_text(raw) if "<" in raw else raw
                    if len(text_clean) > 40:
                        li_date = _extract_best_date(
                            raw if "<" in raw else None,
                            text_clean[:6000], linkedin_url, strict=False)
                        li_text = _build_evidence_text(
                            text_clean, company_name, 480, icp_kw)
                        if li_date and li_text and len(li_text) > 30:
                            ev = {
                                "url": linkedin_url, "source": "linkedin",
                                "extracted_date": li_date,
                                "extracted_text": _trim_sentences(li_text, 480),
                            }
                            ev_desc = ev["extracted_text"]
                            ev_date = li_date
                            chosen_url = linkedin_url

            if not ev_date and ev_desc:
                _stripped = _strip_non_intent_dates(ev_desc)
                _text_dates = _collect_dates_text(_stripped)
                if _text_dates:
                    ev_date = max(_text_dates).isoformat()

            if not ev or not ev_desc or len(ev_desc) < 30:
                continue

            _bare_dl = ev_desc.lower()
            _bare_neg = ("no specific", "no relevant", "no evidence",
                         "lacks specific", "does not", "unable to",
                         "no indication", "no clear", "insufficient",
                         "no signs of", "no mention of")
            if any(p in _bare_dl for p in _bare_neg):
                continue

            if len(ev_desc) < 80:
                has_specific_kw = any(kw in _bare_dl for kw in SPECIFIC_INTENT_KW)
                if not has_specific_kw:
                    continue

            bare_verification = {
                "chosen_url": chosen_url or website or linkedin_url,
                "verified_description": ev_desc[:499],
                "chosen_excerpt": ev_desc[:200],
                "best_date": ev_date,
            }
            bare_out = _build_output(bare_lead, ctx, bare_verification, evidence=[ev])
            if bare_out and _precheck_output(bare_out, ctx):
                best_output = bare_out
                best_score = 0.01
                best_match_features = bare_mfeats
                best_intent_features = _compute_intent_features([ev])
                _debug_log("Bare-minimum fallback succeeded for %s", company_name)
                break

    if best_output is None and top2 and time.time() - t0 < _PIPELINE_BUDGET:
        _debug_log("Super fallback — direct website scrape, no LLM needed")
        for lead, _ in top2:
            if time.time() - t0 > _PIPELINE_BUDGET:
                break
            company = _s(lead.get("business"))
            if not company:
                continue
            try:
                web_url = _s(lead.get("website") or lead.get("company_website"))
                li_url = _s(lead.get("company_linkedin"))
                if web_url and not web_url.startswith("http"):
                    web_url = "https://" + web_url
                if li_url and not li_url.startswith("http"):
                    li_url = "https://" + li_url
                source_url = web_url or li_url
                if not source_url:
                    continue
                cached_html = _page_cache.get(f"{source_url}|False")
                if not cached_html and web_url:
                    cached_html = _scrape(web_url, proxy_url)
                if not cached_html and li_url:
                    cached_html = _fetch_linkedin_company(li_url, proxy_url)
                    source_url = li_url
                if not cached_html or len(cached_html) < 100:
                    continue
                text_clean = _html_to_text(cached_html) if "<" in cached_html else cached_html
                if len(text_clean) < 50:
                    continue
                sf_date = _extract_best_date(
                    cached_html if "<" in cached_html else None,
                    text_clean[:6000], source_url, strict=False)
                if not sf_date:
                    continue
                sf_src = _classify_source(source_url)
                sf_text = _trim_sentences(
                    _build_evidence_text(text_clean, company, 480, icp_kw),
                    480)
                if not sf_text or len(sf_text) < 40:
                    sf_text = text_clean[:480]
                if not sf_text or len(sf_text) < 40:
                    continue
                sf_evidence = [{
                    "url": source_url,
                    "source": sf_src,
                    "extracted_date": sf_date,
                    "extracted_text": sf_text,
                }]
                sf_ver = {
                    "chosen_url": source_url,
                    "verified_description": sf_text[:499],
                    "chosen_excerpt": sf_text[:200],
                    "best_date": sf_date,
                }
                sf_out = _build_output(lead, ctx, sf_ver, evidence=sf_evidence)
                if sf_out and _precheck_output(sf_out, ctx):
                    best_output = sf_out
                    best_score = 0.01
                    best_match_features = _extract_match_features(lead, ctx)
                    best_intent_features = _compute_intent_features(sf_evidence)
                    _debug_log("Super fallback succeeded for %s", company)
                    break
            except Exception:
                continue

    if best_output is None and top2:
        _debug_log("Last chance — simple website text, no LLM")
        for lead, _ in top2[:5]:
            company = _s(lead.get("business"))
            if not company:
                continue
            web_url = _s(lead.get("website") or lead.get("company_website"))
            if not web_url:
                continue
            if not web_url.startswith(("http://", "https://")):
                web_url = "https://" + web_url
            try:
                cached = _page_cache.get(f"{web_url}|False")
                if not cached:
                    cached = _scrape(web_url, proxy_url)
                if not cached or len(cached) < 100:
                    continue
                txt = _html_to_text(cached) if "<" in cached else cached
                if len(txt) < 80:
                    continue
                dt = _extract_best_date(
                    cached if "<" in cached else None,
                    txt[:6000], web_url, strict=False)
                if not dt:
                    continue
                ev_txt = _build_evidence_text(txt, company, 480, None)
                if not ev_txt or len(ev_txt) < 80:
                    ev_txt = txt[:480]
                if not ev_txt or len(ev_txt) < 80:
                    continue
                lc_ev = [{
                    "url": web_url,
                    "source": "company_website",
                    "extracted_date": dt,
                    "extracted_text": _trim_sentences(ev_txt, 480),
                }]
                lc_ver = {
                    "chosen_url": web_url,
                    "verified_description": ev_txt[:499],
                    "chosen_excerpt": ev_txt[:200],
                    "best_date": dt,
                }
                lc_out = _build_output(lead, ctx, lc_ver, evidence=lc_ev)
                if lc_out and _precheck_output(lc_out, ctx):
                    best_output = lc_out
                    best_score = 0.005
                    best_match_features = _extract_match_features(lead, ctx)
                    best_intent_features = _compute_intent_features(lc_ev)
                    _debug_log("Last chance succeeded for %s", company)
                    break
            except Exception:
                continue

    if best_match_features is not None and best_intent_features is not None:
        _learner.log_run(best_match_features, best_intent_features,
                         best_score, icp_context=ctx)
    else:
        _learner.log_none()

    print(f"[SENOO] final: best_output={'YES' if best_output else 'None'}, "
          f"elapsed={time.time()-t0:.1f}s", flush=True)

    if best_output is not None:
        out = best_output

        if "lead_id" not in out or out.get("lead_id") is None:
            lid = out.get("id")
            if lid is not None:
                out["lead_id"] = int(lid)
            else:
                print("[SENOO] EXIT: no lead_id in output", flush=True)
                return None

        if "intent_signals" not in out or not isinstance(out.get("intent_signals"), list):
            old_sig = out.pop("intent_signal", None)
            if isinstance(old_sig, dict) and old_sig:
                out["intent_signals"] = [old_sig]
            else:
                print("[SENOO] EXIT: no intent_signals in output", flush=True)
                return None

        out["lead_id"] = int(out["lead_id"])

        _sigs = out.get("intent_signals", [])
        if _sigs and isinstance(_sigs, list):
            _pri = _sigs[0] if _sigs else {}
            _pri_desc = (_pri.get("description") or "").lower()
            _emit_block = ("no specific", "no relevant", "no evidence",
                           "lacks specific", "no indication", "no clear",
                           "does not indicate", "unable to", "insufficient",
                           "does not show", "not enough evidence",
                           "no signs of", "no mention of", "no data on")
            if any(p in _pri_desc for p in _emit_block):
                _debug_log("Negative desc blocked, attempting desc repair")
                _pri["description"] = re.sub(
                    r'(?:no specific|no relevant|no evidence|lacks specific|'
                    r'no indication|no clear|does not indicate|unable to|'
                    r'insufficient|does not show|not enough evidence|'
                    r'no signs of|no mention of|no data on)[^.]*\.?\s*',
                    '', _pri.get("description", ""), flags=re.I).strip()
                _pri_desc = _pri["description"].lower()
                if len(_pri_desc) < 30:
                    print("[SENOO] EXIT: negative desc too short after repair", flush=True)
                    return None

        lid = out.get("lead_id") or out.get("id")
        if lid is not None:
            _seen_lead_ids.add(lid)
        biz = _sl(out.get("business", "")).strip()
        if biz:
            _seen_companies.add(biz)

        print(f"[SENOO] SUCCESS: returning lead {out.get('business')}, id={lid}", flush=True)
        return out

    print("[SENOO] EXIT: best_output is None after all fallbacks", flush=True)
    return None

qualify = find_leads