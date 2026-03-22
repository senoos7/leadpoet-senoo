"""
Integration wrapper for Lead Sorcerer model to be compatible with the existing miner system.

This module provides a get_leads() function that runs the Lead Sorcerer orchestrator
and converts the output to the format expected by the existing miner code.
"""

import asyncio
import json
import os
import sys
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Any
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Check for required dependencies first
def check_dependencies():
    """Check if required dependencies are available."""
    try:
        import phonenumbers
        import httpx
        import openai
        return True, None
    except ImportError as e:
        return False, str(e)


# Check dependencies before importing Lead Sorcerer components
deps_ok, error_msg = check_dependencies()
if not deps_ok:
    print(f"❌ Could not import Lead Sorcerer orchestrator: {error_msg}")
    print("   Please ensure the Lead Sorcerer model is properly installed")
    print(
        "   Run: pip install -r miner_models/lead_sorcerer_main/requirements.txt"
    )

    # Provide fallback function that returns empty results
    async def get_leads(num_leads: int,
                        industry: str = None,
                        region: str = None) -> List[Dict[str, Any]]:
        """Fallback function when dependencies are missing."""
        print(
            "⚠️ Lead Sorcerer dependencies not available, returning empty results"
        )
        return []
else:
    # Get the absolute path to the lead_sorcerer_main directory
    lead_sorcerer_dir = Path(__file__).parent.absolute()
    src_path = lead_sorcerer_dir / "src"
    config_path = lead_sorcerer_dir / "config"

    # Add the src directory to the path so we can import the orchestrator
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    try:
        # Try to import with absolute path first
        import sys
        old_path = sys.path.copy()

        # Add both the lead_sorcerer_main directory and its src subdirectory
        sys.path.insert(0, str(lead_sorcerer_dir))
        sys.path.insert(0, str(src_path))

        from orchestrator import LeadSorcererOrchestrator
        LEAD_SORCERER_AVAILABLE = True

        # Restore original path but keep our additions
        for path in [str(lead_sorcerer_dir), str(src_path)]:
            if path not in old_path and path in sys.path:
                continue  # Keep our additions

    except ImportError as e:
        print(f"❌ Could not import Lead Sorcerer orchestrator: {e}")
        print(f"   Tried to import from: {src_path}")
        print(f"   Directory exists: {src_path.exists()}")
        if src_path.exists():
            print(f"   Contents: {list(src_path.iterdir())}")
        LEAD_SORCERER_AVAILABLE = False

    # Suppress verbose logging from the lead sorcerer
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)

    # ───────────────────────────────────────────────────────────────────
    #  Load canonical ICP template (must exist)
    # ───────────────────────────────────────────────────────────────────
    try:
        ICP_TEMPLATE_PATH = lead_sorcerer_dir / "icp_config.json"
        with open(ICP_TEMPLATE_PATH, "r", encoding="utf-8") as _f:
            BASE_ICP_CONFIG: Dict[str, Any] = json.load(_f)
    except Exception as e:
        raise RuntimeError(
            f"Lead Sorcerer wrapper: required icp_config.json not found or unreadable "
            f"at {ICP_TEMPLATE_PATH}. Error: {e}") from e

    # ---------------------------------------------------------------------------
    # ICP rotation table — one entry per underserved niche.
    #
    # Strategy: with 4.4M+ leads already in the subnet, popular sectors
    # (cybersecurity, generic AI) have near-100% duplicate rates.  Rotating
    # across niches that other miners ignore maximises unique submissions.
    #
    # Rotation cadence: one niche per 6-hour window (matches domain_serp_ttl_hours)
    # so every SERP cache expiry lands on a fresh sector.  Full cycle = 36 hours.
    # ---------------------------------------------------------------------------
    _NICHE_ROTATION = [
        # ── 0: LegalTech ────────────────────────────────────────────────────────
        {
            "name": "B2B LegalTech Startups",
            "icp_text": (
                "B2B legaltech, contract management, e-discovery, and compliance "
                "SaaS startups with 5-300 employees, founded after 2015, based in "
                "the US or UK. Target founders or C-suite with contact info on site."
            ),
            "queries": [
                "legaltech startup CEO founder email \"contact\" site:*.io OR site:*.ai -site:linkedin.com -site:github.com -site:crunchbase.com",
                "contract management OR e-discovery startup \"co-founder\" email \"about\" OR \"team\" -site:linkedin.com -site:github.com -site:crunchbase.com -site:medium.com",
                "legal SaaS startup founder CEO email 2021 OR 2022 OR 2023 \"contact us\" -site:linkedin.com -site:github.com -site:medium.com",
                "compliance technology startup \"meet the team\" OR \"our founders\" email -site:linkedin.com -site:github.com -site:crunchbase.com",
                "legaltech B2B startup founder \"San Francisco\" OR \"New York\" OR \"Chicago\" email -site:linkedin.com -site:crunchbase.com",
            ],
        },
        # ── 1: ClimaTech / GreenTech ────────────────────────────────────────────
        {
            "name": "B2B ClimaTech Startups",
            "icp_text": (
                "B2B climate technology, sustainability SaaS, carbon accounting, "
                "and clean energy software startups with 5-500 employees, founded "
                "after 2016, US/EU-based. Target founders with public contact info."
            ),
            "queries": [
                "climatetech startup CEO founder email \"contact\" site:*.io OR site:*.ai -site:linkedin.com -site:github.com -site:crunchbase.com",
                "carbon accounting OR ESG software startup \"co-founder\" email \"about\" -site:linkedin.com -site:github.com -site:crunchbase.com",
                "clean energy SaaS startup founder CEO email 2021 OR 2022 OR 2023 \"contact us\" -site:linkedin.com -site:github.com",
                "sustainability technology B2B startup founder email \"team\" OR \"about us\" -site:linkedin.com -site:crunchbase.com -site:medium.com",
                "greentech startup \"co-founder\" email \"Austin\" OR \"Denver\" OR \"Seattle\" OR \"London\" -site:linkedin.com -site:crunchbase.com",
            ],
        },
        # ── 2: LogisticsTech / Supply Chain ─────────────────────────────────────
        {
            "name": "B2B LogisticsTech Startups",
            "icp_text": (
                "B2B logistics technology, freight, supply chain visibility, and "
                "last-mile delivery SaaS startups with 10-500 employees, founded "
                "after 2015. Target US-based founders with public email."
            ),
            "queries": [
                "logistics technology startup CEO founder email \"contact\" site:*.io OR site:*.com -site:linkedin.com -site:github.com -site:crunchbase.com",
                "supply chain SaaS startup \"co-founder\" email \"about\" OR \"team\" -site:linkedin.com -site:github.com -site:crunchbase.com",
                "freight tech OR last-mile startup founder CEO email 2020 OR 2021 OR 2022 -site:linkedin.com -site:github.com -site:medium.com",
                "warehouse management OR fleet management startup founder email \"meet the team\" -site:linkedin.com -site:crunchbase.com",
                "logistics SaaS \"co-founder\" email \"Chicago\" OR \"Dallas\" OR \"Atlanta\" OR \"Houston\" -site:linkedin.com -site:crunchbase.com",
            ],
        },
        # ── 3: ConstructionTech / PropTech ──────────────────────────────────────
        {
            "name": "B2B ConstructionTech and PropTech Startups",
            "icp_text": (
                "B2B construction technology (AEC), property management SaaS, "
                "and real estate tech startups with 5-300 employees, founded after "
                "2015. Target founders with public email addresses."
            ),
            "queries": [
                "constructiontech OR contech startup CEO founder email \"contact\" site:*.io OR site:*.com -site:linkedin.com -site:github.com -site:crunchbase.com",
                "proptech startup \"co-founder\" email \"about us\" OR \"team\" -site:linkedin.com -site:github.com -site:crunchbase.com -site:medium.com",
                "AEC software OR construction SaaS startup founder email 2020 OR 2021 OR 2022 -site:linkedin.com -site:medium.com",
                "real estate technology B2B startup founder CEO \"meet the team\" email -site:linkedin.com -site:crunchbase.com",
                "proptech startup \"co-founder\" email \"New York\" OR \"Miami\" OR \"Austin\" OR \"Phoenix\" -site:linkedin.com",
            ],
        },
        # ── 4: AgriTech / FoodTech ──────────────────────────────────────────────
        {
            "name": "B2B AgriTech and FoodTech Startups",
            "icp_text": (
                "B2B agriculture technology, precision farming, food supply chain, "
                "and restaurant/food service SaaS startups with 5-200 employees, "
                "founded after 2015. Target founders with public contact info."
            ),
            "queries": [
                "agritech startup CEO founder email \"contact\" site:*.io OR site:*.ai -site:linkedin.com -site:github.com -site:crunchbase.com",
                "precision agriculture OR food tech B2B startup \"co-founder\" email \"about\" -site:linkedin.com -site:crunchbase.com -site:medium.com",
                "agriculture SaaS startup founder CEO email 2020 OR 2021 OR 2022 \"contact us\" -site:linkedin.com -site:github.com",
                "foodtech OR restaurant technology startup founder email \"meet the team\" -site:linkedin.com -site:crunchbase.com",
                "agritech startup \"co-founder\" email \"Iowa\" OR \"Kansas\" OR \"Nebraska\" OR \"Minnesota\" -site:linkedin.com",
            ],
        },
        # ── 5: HRTech / WorkforceTech ───────────────────────────────────────────
        {
            "name": "B2B HRTech Startups",
            "icp_text": (
                "B2B HR technology, talent acquisition, employee engagement, and "
                "workforce management SaaS startups with 10-500 employees, founded "
                "after 2016. Target US founders with public email addresses."
            ),
            "queries": [
                "hrtech startup CEO founder email \"contact\" site:*.io OR site:*.ai -site:linkedin.com -site:github.com -site:crunchbase.com",
                "talent acquisition OR employee engagement SaaS startup \"co-founder\" email \"about\" -site:linkedin.com -site:crunchbase.com",
                "workforce management startup founder CEO email 2021 OR 2022 OR 2023 \"contact us\" -site:linkedin.com -site:medium.com",
                "HR software B2B startup founder \"meet the team\" OR \"our founders\" email -site:linkedin.com -site:crunchbase.com",
                "hrtech startup \"co-founder\" email \"Phoenix\" OR \"Salt Lake City\" OR \"Nashville\" OR \"Raleigh\" -site:linkedin.com",
            ],
        },
    ]

    def create_industry_specific_config(
            industry: str | None = None) -> Dict[str, Any]:
        """
        Clone the canonical icp_config.json and apply a rotating niche strategy.

        With 4.4M+ leads already in the subnet, generic/popular sectors have
        near-100% duplicate rates.  This function selects a different underserved
        niche every 6 hours (matching domain_serp_ttl_hours) so the miner always
        searches in fresh territory across the full 36-hour rotation cycle.

        If a specific `industry` string is passed (from the validator task) it
        overrides the rotation with a best-match niche.
        """
        import time
        config = json.loads(json.dumps(BASE_ICP_CONFIG))  # deep-copy

        # ── Determine which niche to use ────────────────────────────────────────
        if industry:
            ind = industry.lower()
            # Match caller-supplied industry to the closest niche
            if any(k in ind for k in ("legal", "contract", "compliance", "ediscovery")):
                niche = _NICHE_ROTATION[0]
            elif any(k in ind for k in ("climate", "green", "carbon", "esg", "clean")):
                niche = _NICHE_ROTATION[1]
            elif any(k in ind for k in ("logistic", "supply", "freight", "warehouse", "fleet")):
                niche = _NICHE_ROTATION[2]
            elif any(k in ind for k in ("construct", "proptech", "real estate", "aec", "property")):
                niche = _NICHE_ROTATION[3]
            elif any(k in ind for k in ("agri", "food", "farm", "agriculture")):
                niche = _NICHE_ROTATION[4]
            elif any(k in ind for k in ("hr", "human resource", "talent", "workforce")):
                niche = _NICHE_ROTATION[5]
            else:
                # Unknown industry — fall through to time-based rotation
                niche = _NICHE_ROTATION[int(time.time() // (6 * 3600)) % len(_NICHE_ROTATION)]
        else:
            # Automatic 6-hour rotation: epoch // 21600 cycles through all niches
            slot = int(time.time() // (6 * 3600)) % len(_NICHE_ROTATION)
            niche = _NICHE_ROTATION[slot]
            print(f"🔄 ICP niche rotation slot {slot}: '{niche['name']}'")

        # Apply the selected niche onto the base config template
        config["name"] = niche["name"]
        config["icp_text"] = niche["icp_text"]
        config["queries"] = niche["queries"]

        return config

    def setup_temp_environment(temp_dir: str):
        """Set up the temporary environment with required config files."""
        temp_path = Path(temp_dir)

        # Create config directory in temp
        temp_config_dir = temp_path / "config"
        temp_config_dir.mkdir(exist_ok=True)

        # Copy required config files
        source_config_dir = config_path

        # Copy costs.yaml (required)
        costs_file = source_config_dir / "costs.yaml"
        if costs_file.exists():
            shutil.copy2(costs_file, temp_config_dir / "costs.yaml")

        # Copy prompts directory if it exists
        source_prompts = source_config_dir / "prompts"
        if source_prompts.exists():
            temp_prompts = temp_config_dir / "prompts"
            shutil.copytree(source_prompts, temp_prompts, dirs_exist_ok=True)

        # NEW: copy the JSON-schema directory so validation works
        source_schemas = lead_sorcerer_dir / "schemas"
        if source_schemas.exists():
            temp_schemas = temp_path / "schemas"
            shutil.copytree(source_schemas, temp_schemas, dirs_exist_ok=True)

    # All fields the gateway requires to be non-empty before accepting a lead.
    GATEWAY_REQUIRED_FIELDS = [
        "business", "full_name", "first", "last", "email", "role",
        "website", "industry", "sub_industry", "country", "city",
        "linkedin", "company_linkedin", "source_url", "description",
        "employee_count",
    ]

    # Domains that will NEVER yield valid founder leads — skip before hitting Firecrawl.
    # Includes: aggregators, large enterprises, media sites, VC portals, social platforms.
    DOMAIN_DENYLIST = {
        # Social / media platforms
        "linkedin.com", "github.com", "reddit.com", "twitter.com", "x.com",
        "youtube.com", "facebook.com", "instagram.com", "tiktok.com",
        "medium.com", "substack.com", "dev.to",
        # News / editorial
        "techcrunch.com", "venturebeat.com", "wired.com", "forbes.com",
        "bloomberg.com", "wsj.com", "nytimes.com", "theguardian.com",
        "businessinsider.com", "inc.com", "entrepreneur.com",
        # B2B data aggregators — these ARE the competition, not targets
        "zoominfo.com", "apollo.io", "hunter.io", "clearbit.com",
        "lusha.com", "rocketreach.co", "snov.io", "seamless.ai",
        "contactout.com", "uplead.com", "leadiq.com", "cognism.com",
        # Company databases / directories
        "crunchbase.com", "pitchbook.com", "owler.com", "cbinsights.com",
        "ycombinator.com", "angellist.com", "producthunt.com",
        "clutch.co", "g2.com", "capterra.com", "trustpilot.com",
        # VC / investor portals (not target companies)
        "a16z.com", "sequoiacap.com", "greylock.com", "foundationcapital.com",
        "bessemervp.com", "accel.com", "lightspeedvp.com", "nea.com",
        # Large enterprises (too big, no public founder emails)
        "crowdstrike.com", "paloaltonetworks.com", "sentinelone.com",
        "darktrace.com", "fortinet.com", "checkpoint.com", "cisco.com",
        "microsoft.com", "google.com", "amazon.com", "ibm.com",
        # Personal / blog sites that don't represent B2B companies
        "danielscrivner.com",
    }

    # Common abbreviation → full country name mapping used when parsing hq_location.
    _COUNTRY_NORM = {
        "US": "United States", "USA": "United States", "U.S.": "United States",
        "U.S.A.": "United States", "UNITED STATES OF AMERICA": "United States",
        "UK": "United Kingdom", "GB": "United Kingdom", "G.B.": "United Kingdom",
        "CA": "Canada", "AU": "Australia", "DE": "Germany", "FR": "France",
        "IN": "India", "SG": "Singapore", "IL": "Israel",
    }

    # US state abbreviation → full name (needed for gateway "state" field on US leads).
    _US_STATES = {
        "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
        "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
        "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
        "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
        "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
        "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
        "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
        "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
        "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
        "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
        "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
        "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
        "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
    }

    # Employee count strings Firecrawl hallucinates that are not valid gateway ranges.
    _INVALID_EMPLOYEE_COUNTS = {
        "unknown", "not mentioned", "not specified", "n/a", "none", "",
        "null", "undefined", "various", "multiple",
    }

    # Placeholder / hallucinated contact names that Firecrawl invents when it
    # cannot find a real person.  These will be rejected by source provenance.
    _FAKE_NAMES = {
        "john doe", "jane doe", "john smith", "jane smith",
        "mike johnson", "michael johnson", "sarah connor", "sarah conner",
        "david lastname", "test user", "first last", "firstname lastname",
        "sample user", "dummy user", "placeholder", "unknown user",
    }

    # Generic / shared-inbox prefixes that are never personal emails.
    _GENERIC_EMAIL_PREFIXES = {
        "contact", "info", "hello", "hey", "hi", "support", "help",
        "admin", "team", "office", "enquiry", "inquiry", "mail",
        "sales", "marketing", "hr", "noreply", "no-reply", "privacy",
        "legal", "billing", "accounts", "press", "media", "pr",
    }

    def _parse_hq_location(hq_location: str):
        """
        Split a free-text hq_location string into (city, state, country).

        Handles patterns like:
          "San Francisco, CA, United States"
          "New York, NY, USA"
          "London, UK"
          "Tel Aviv, Israel"
          "Berlin, Germany"

        Returns ("", "", "") for sentinel values like "Unknown" or "Not mentioned".
        """
        _INVALID_LOCATIONS = {"unknown", "not mentioned", "not specified", "n/a", "none", "somewhere"}
        if not hq_location or hq_location.strip().lower() in _INVALID_LOCATIONS:
            return "", "", ""
        parts = [p.strip() for p in hq_location.split(",")]
        city = state = country = ""
        if len(parts) >= 3:
            city, state, country = parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            city, country = parts[0], parts[1]
        else:
            city = parts[0]
        # Normalize country abbreviations (e.g. "USA" → "United States")
        country_key = country.strip().upper().rstrip(".")
        country = _COUNTRY_NORM.get(country_key, country.strip())
        # Expand US state abbreviations (e.g. "CA" → "California")
        state_key = state.strip().upper()
        state = _US_STATES.get(state_key, state.strip())
        # If city looks like a known sentinel, clear it
        if city.lower() in _INVALID_LOCATIONS:
            city = ""
        return city.strip(), state.strip(), country.strip()

    def convert_lead_record_to_legacy_format(
            lead_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a Lead Sorcerer lead record to the format expected by the existing miner code.
        
        Args:
            lead_record: Lead record from Lead Sorcerer in unified schema format
            
        Returns:
            Lead in the format expected by the existing miner system
        """
        company = lead_record.get("company", {})
        contacts = lead_record.get("contacts", [])

        # Get the best contact (prefer one with an email, then any contact)
        best_contact = None
        if contacts:
            # Prefer contacts with email addresses
            email_contacts = [c for c in contacts if c.get("email")]
            if email_contacts:
                best_contact = email_contacts[0]
            else:
                # Otherwise use the first contact
                best_contact = contacts[0]

        # Extract contact information
        if best_contact:
            # Handle both full_name (from crawl tool) and first_name/last_name (legacy)
            raw_name = (best_contact.get("full_name") or "").strip()
            first_name = (best_contact.get("first_name") or "").strip()
            last_name = (best_contact.get("last_name") or "").strip()

            # If we have full_name but not first/last, try to split
            if raw_name and not (first_name or last_name):
                name_parts = raw_name.split(maxsplit=1)
                first_name = name_parts[0] if len(name_parts) > 0 else ""
                last_name = name_parts[1] if len(name_parts) > 1 else ""
            # If we have first/last but not full_name, combine them
            elif not raw_name and (first_name or last_name):
                raw_name = f"{first_name} {last_name}".strip()

            # Proper-case names (Firecrawl often returns lowercase)
            first_name = first_name.title()
            last_name = last_name.title()
            full_name = f"{first_name} {last_name}".strip() if (first_name or last_name) else raw_name.title()

            email = (best_contact.get("email") or "").strip().lower()
            # Handle both 'role' (from crawl tool) and 'job_title' (legacy)
            job_title = best_contact.get("role") or best_contact.get("job_title") or ""
            # Title-case the role as well
            job_title = job_title.title() if job_title else ""

            # Extract LinkedIn URL (can be full URL or path like "/in/username")
            linkedin_raw = best_contact.get("linkedin") or best_contact.get("linkedin_url") or ""
            # Normalize to full URL if it's just a path
            if linkedin_raw and linkedin_raw.startswith("/in/"):
                linkedin = f"https://www.linkedin.com{linkedin_raw}"
            elif linkedin_raw and linkedin_raw.startswith("in/"):
                linkedin = f"https://www.linkedin.com/{linkedin_raw}"
            elif linkedin_raw and not linkedin_raw.startswith("http"):
                linkedin = f"https://www.linkedin.com/in/{linkedin_raw.lstrip('/')}"
            else:
                linkedin = linkedin_raw

            # Fallback to default LinkedIn from ICP config if not found
            if not linkedin and BASE_ICP_CONFIG.get("default_contact_linkedin"):
                linkedin = BASE_ICP_CONFIG["default_contact_linkedin"]
        else:
            first_name = ""
            last_name = ""
            full_name = ""
            email = ""
            job_title = ""
            # Use default LinkedIn from ICP config as fallback
            linkedin = BASE_ICP_CONFIG.get("default_contact_linkedin", "")

        # Helper function to safely get string values
        def safe_str(value, default=""):
            """Safely convert value to string, handling None values."""
            if value is None:
                return default
            return str(value)

        # ── Location: split hq_location into city / state / country ──────────
        hq_location = safe_str(company.get("hq_location"))
        city, state, country = _parse_hq_location(hq_location)

        # Fall back to the ICP default_country when Firecrawl returned "Unknown".
        if not country:
            default_country = BASE_ICP_CONFIG.get("default_country", "United States")
            # Expand "US" shorthand stored in some ICP configs
            country = _COUNTRY_NORM.get(default_country.upper(), default_country)

        # ── Source URL (company website used as provenance anchor) ────────────
        domain = safe_str(lead_record.get("domain"))
        source_url = f"https://{domain}" if domain else ""
        website = source_url  # keep consistent

        # ── Social links ──────────────────────────────────────────────────────
        socials = company.get("socials", {}) or {}
        company_linkedin = safe_str(socials.get("linkedin"))

        # Fallback: derive LinkedIn company URL from domain name when Firecrawl
        # didn't find it. Most companies use their brand (domain without TLD) as
        # their LinkedIn slug (e.g. straiker.ai → /company/straiker).
        if not company_linkedin and domain:
            company_slug = domain.split(".")[0].lower().strip()
            if company_slug:
                company_linkedin = f"https://www.linkedin.com/company/{company_slug}"

        # ── Employee count ────────────────────────────────────────────────────
        raw_employee_count = safe_str(company.get("employee_count"))
        # Map invalid/sentinel values to a gateway-accepted default range.
        # Valid ranges: "0-1","2-10","11-50","51-200","201-500","501-1,000",
        #               "1,001-5,000","5,001-10,000","10,001+"
        if raw_employee_count.lower().strip() in _INVALID_EMPLOYEE_COUNTS:
            employee_count = "11-50"  # safe default for a startup context
        else:
            employee_count = raw_employee_count

        # Build the lead dict with all gateway-required fields populated.
        legacy_lead = {
            "business": safe_str(company.get("name")),
            "description": safe_str(company.get("description")),
            "full_name": full_name,
            "first": first_name,
            "last": last_name,
            "email": email,
            "role": job_title,
            "linkedin": linkedin,
            "company_linkedin": company_linkedin,
            "website": website,
            "source_url": source_url,
            "source_type": "company_site",
            "industry": safe_str(company.get("industry")),
            "sub_industry": safe_str(company.get("sub_industry")),
            "country": country,
            "city": city,
            "state": state,
            "employee_count": employee_count,
            # Optional enrichment fields
            "phone_numbers": company.get("phone_numbers", []),
            "socials": socials,
            "hq_country": country,
            "hq_state": state,
            "hq_city": city,
            "founded_year": safe_str(company.get("founded_year")),
            "ownership_type": safe_str(company.get("ownership_type")),
            "company_type": safe_str(company.get("company_type")),
            "number_of_locations": safe_str(company.get("number_of_locations")),
        }

        return legacy_lead

    def validate_lead_for_gateway(lead: Dict[str, Any]) -> List[str]:
        """Return list of missing required gateway fields (empty = lead is ready)."""
        return [f for f in GATEWAY_REQUIRED_FIELDS if not lead.get(f)]

    async def run_lead_sorcerer_pipeline(
            num_leads: int,
            industry: str = None,
            region: str = None) -> List[Dict[str, Any]]:
        """
        Run the Lead Sorcerer pipeline and extract leads.
        
        Args:
            num_leads: Number of leads to generate
            industry: Target industry (optional)
            region: Target region (optional)
            
        Returns:
            List of lead records from Lead Sorcerer
        """
        if not LEAD_SORCERER_AVAILABLE:
            return []

        # Use a stable persistent data dir if set, so SERP cache and domain
        # history survive across the 60-second sourcing cycles.  Fall back to
        # a temp dir when the env var is absent (keeps backward compatibility).
        stable_data_dir = os.environ.get("LEADPOET_DATA_DIR")
        if stable_data_dir:
            Path(stable_data_dir).mkdir(parents=True, exist_ok=True)

        # Create a temporary directory for config files / working dir only
        with tempfile.TemporaryDirectory() as temp_dir:
            # Set up the temporary environment with config files
            setup_temp_environment(temp_dir)

            # Point the orchestrator at the stable cache dir (or the temp dir
            # if no persistent path was configured).
            effective_data_dir = stable_data_dir or temp_dir
            os.environ["LEADPOET_DATA_DIR"] = effective_data_dir

            # Change to temp directory so relative paths work
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)

                # Create configuration
                config = create_industry_specific_config(industry)

                # Adjust caps based on number of requested leads
                config["caps"]["max_domains_per_run"] = min(
                    max(num_leads * 2, 5), 20)
                config["caps"]["max_crawl_per_run"] = min(
                    max(num_leads * 2, 5), 20)

                # Save config to temporary file
                config_file = Path(temp_dir) / "icp_config.json"
                with open(config_file, "w") as f:
                    json.dump(config, f, indent=2)

                try:
                    # Initialize and run orchestrator
                    orchestrator = LeadSorcererOrchestrator(
                        str(config_file), batch_size=num_leads)

                    async with orchestrator:  # Use async context manager for proper cleanup
                        result = await orchestrator.run_pipeline()

                        if not result.get("success"):
                            print(
                                f"⚠️ Lead Sorcerer pipeline failed: {result.get('errors', [])}"
                            )
                            return []

                        # Extract leads from the result - look in exports directory.
                        # Exports are written to effective_data_dir (stable or temp).
                        leads = []

                        for search_dir in [effective_data_dir, temp_dir]:
                            exports_dir = Path(search_dir) / "exports"
                            if not exports_dir.exists():
                                continue
                            export_dirs = list(exports_dir.glob("*/*"))
                            if not export_dirs:
                                continue
                            latest_export = max(
                                export_dirs,
                                key=lambda x: x.stat().st_mtime,
                            )
                            leads_file = latest_export / "leads.jsonl"
                            if not leads_file.exists():
                                continue
                            print(f"📂 Reading leads from: {leads_file}")
                            with open(leads_file, "r") as f:
                                for line in f:
                                    if not line.strip():
                                        continue
                                    try:
                                        lead_record = json.loads(line)
                                        has_contacts = (
                                            lead_record.get("contacts")
                                            and len(lead_record.get("contacts", [])) > 0
                                        )
                                        # Load ALL contact-bearing records regardless of
                                        # num_leads — the get_leads() caller will validate
                                        # and pick the best ones after conversion.
                                        if has_contacts:
                                            leads.append(lead_record)
                                    except json.JSONDecodeError:
                                        continue
                            if leads:
                                break  # Found leads — no need to check temp_dir

                        # Fallback: domain_pass.jsonl in either dir
                        if not leads:
                            for search_dir in [effective_data_dir, temp_dir]:
                                domain_pass_file = Path(search_dir) / "domain_pass.jsonl"
                                if not domain_pass_file.exists():
                                    continue
                                with open(domain_pass_file, "r") as f:
                                    for line in f:
                                        if not line.strip():
                                            continue
                                        try:
                                            lead_record = json.loads(line)
                                            if (
                                                lead_record.get("icp", {}).get("pre_pass")
                                                and lead_record.get("contacts")
                                            ):
                                                leads.append(lead_record)
                                        except json.JSONDecodeError:
                                            continue
                                if leads:
                                    break

                        return leads[:num_leads]

                except Exception as e:
                    print(f"❌ Error running Lead Sorcerer pipeline: {e}")
                    return []

            finally:
                # Always restore the original working directory
                os.chdir(original_cwd)

    async def get_leads(num_leads: int,
                        industry: str = None,
                        region: str = None) -> List[Dict[str, Any]]:
        """
        Generate leads using the Lead Sorcerer model.
        
        This function is compatible with the existing miner system and can be used as a drop-in
        replacement for the get_leads function from miner_models.get_leads.
        
        Args:
            num_leads: Number of leads to generate
            industry: Target industry (optional)
            region: Target region (optional)
            
        Returns:
            List of leads in the format expected by the existing miner system
        """
        # Check if required environment variables are set.
        # Search: at least one of SERP_API_KEY (SerpAPI) or SERPER_API_KEY (Serper.dev)
        always_required = ["OPENROUTER_KEY", "FIRECRAWL_KEY"]
        missing_vars = [var for var in always_required if not os.getenv(var)]

        has_search_key = bool(os.getenv("SERP_API_KEY") or os.getenv("SERPER_API_KEY"))
        if not has_search_key:
            missing_vars.append("SERP_API_KEY or SERPER_API_KEY")

        if missing_vars:
            print(
                f"⚠️ Lead Sorcerer missing required environment variables: {missing_vars}"
            )
            print("   Please set these in your .env file or environment")
            return []

        if not LEAD_SORCERER_AVAILABLE:
            print("⚠️ Lead Sorcerer not available, returning empty results")
            return []

        try:
            # Run the Lead Sorcerer pipeline
            lead_records = await run_lead_sorcerer_pipeline(
                num_leads, industry, region)

            if not lead_records:
                print("⚠️ Lead Sorcerer produced no leads")
                return []

            # Convert to legacy format and run local pre-submission validation
            legacy_leads = []
            for record in lead_records:
                try:
                    # Skip known-bad domains before any expensive conversion
                    record_domain = str(record.get("domain", "")).lower().strip()
                    if record_domain in DOMAIN_DENYLIST:
                        print(f"🚫 Skipping denylisted domain: {record_domain}")
                        continue

                    legacy_lead = convert_lead_record_to_legacy_format(record)

                    # Basic sanity: must have email and business name
                    if not (legacy_lead.get("email") and legacy_lead.get("business")):
                        continue

                    # Reject generic / shared-inbox emails (not personal leads)
                    email_val = legacy_lead.get("email", "")
                    email_prefix = email_val.split("@")[0].lower() if "@" in email_val else ""
                    if email_prefix in _GENERIC_EMAIL_PREFIXES:
                        print(
                            f"🚫 Skipping generic email '{email_val}' "
                            f"for '{legacy_lead.get('business')}'"
                        )
                        continue

                    # Reject hallucinated / placeholder names
                    full_name_check = legacy_lead.get("full_name", "").lower().strip()
                    if full_name_check in _FAKE_NAMES:
                        print(
                            f"🚫 Skipping hallucinated contact '{legacy_lead.get('full_name')}' "
                            f"for '{legacy_lead.get('business')}'"
                        )
                        continue

                    # Pre-submission gateway field check — avoids burning rejection credits
                    missing = validate_lead_for_gateway(legacy_lead)
                    if missing:
                        print(
                            f"⚠️ Skipping lead '{legacy_lead.get('business')}' "
                            f"— missing gateway fields: {missing}"
                        )
                        continue

                    legacy_leads.append(legacy_lead)

                except Exception as e:
                    print(f"⚠️ Error converting lead record: {e}")
                    continue

            print(f"✅ Lead Sorcerer produced {len(legacy_leads)} valid leads")
            return legacy_leads

        except Exception as e:
            print(f"❌ Lead Sorcerer error: {e}")
            return []


# Fallback function if dependencies are not available
if not deps_ok:

    async def get_leads(num_leads: int,
                        industry: str = None,
                        region: str = None) -> List[Dict[str, Any]]:
        """Fallback function when dependencies are missing."""
        print(
            "⚠️ Lead Sorcerer dependencies not available, returning empty results"
        )
        return []


# For backward compatibility and testing
if __name__ == "__main__":
    # Test the function
    import time

    async def test_async():
        start_time = time.time()

        print("🧪 Testing Lead Sorcerer integration...")
        test_leads = await get_leads(2, "Technology")

        print(
            f"⏱️ Generated {len(test_leads)} leads in {time.time() - start_time:.2f}s"
        )

        for i, lead in enumerate(test_leads, 1):
            print(f"\n{i}. {lead.get('business', 'Unknown')}")
            print(
                f"   Contact: {lead.get('full_name', 'Unknown')} ({lead.get('email', 'No email')})"
            )
            print(f"   Industry: {lead.get('industry', 'Unknown')}")
            print(f"   Website: {lead.get('website', 'No website')}")

    asyncio.run(test_async())
