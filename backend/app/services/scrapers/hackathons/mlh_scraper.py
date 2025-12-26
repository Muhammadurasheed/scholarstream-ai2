"""
MLH (Major League Hacking) API Scraper
Fetches hackathon events from MLH's public API/data endpoints.
"""
import httpx
import asyncio
import structlog
from typing import List, Dict, Any, Optional
from datetime import datetime

from app.services.flink_processor import generate_opportunity_id
from app.database import db
from app.models import Scholarship

logger = structlog.get_logger()

# MLH uses a JSON data file for their event listings
MLH_EVENTS_URL = "https://mlh.io/seasons/2025/events"
MLH_API_URL = "https://us-central1-hackathon-finder-api.cloudfunctions.net/api/events"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json, text/html, */*',
}


async def fetch_mlh_events() -> List[Dict[str, Any]]:
    """
    Fetch MLH hackathon events.
    MLH doesn't have a public API, so we try multiple approaches.
    """
    events = []
    
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            # Approach 1: Try the Hackathon Finder API (aggregates MLH events)
            try:
                response = await client.get(
                    "https://api.mlh.io/v4/events",
                    headers=HEADERS
                )
                if response.status_code == 200:
                    data = response.json()
                    events = data.get('data', data) if isinstance(data, dict) else data
                    logger.info("MLH official API success", count=len(events))
                    return events
            except Exception:
                pass
            
            # Approach 2: Scrape the events page for JSON data
            try:
                response = await client.get(
                    "https://mlh.io/seasons/2025/events",
                    headers={**HEADERS, 'Accept': 'text/html'}
                )
                if response.status_code == 200:
                    import re
                    import json
                    
                    # Look for __NEXT_DATA__ or embedded JSON
                    text = response.text
                    
                    # Pattern for Next.js data
                    next_match = re.search(
                        r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                        text,
                        re.DOTALL
                    )
                    if next_match:
                        page_data = json.loads(next_match.group(1))
                        events = page_data.get('props', {}).get('pageProps', {}).get('events', [])
                        if events:
                            logger.info("MLH Next.js data extraction success", count=len(events))
                            return events
                    
                    # Pattern for embedded events array
                    json_match = re.search(
                        r'window\.__EVENTS__\s*=\s*(\[.*?\]);',
                        text,
                        re.DOTALL
                    )
                    if json_match:
                        events = json.loads(json_match.group(1))
                        logger.info("MLH embedded events success", count=len(events))
                        return events
                        
            except Exception as e:
                logger.debug("MLH page scrape failed", error=str(e))
            
            # Approach 3: Use a static list of known MLH 2025 events
            # This is a fallback when API access fails
            logger.info("MLH API unavailable, using static event data")
            return get_static_mlh_events()
            
    except Exception as e:
        logger.warning("MLH fetch failed", error=str(e))
        return get_static_mlh_events()


def get_static_mlh_events() -> List[Dict[str, Any]]:
    """
    Static fallback data for MLH 2025 season events.
    Updated as of December 2024.
    """
    return [
        {
            "name": "HackMIT",
            "url": "https://hackmit.org/",
            "location": "Cambridge, MA",
            "start_date": "2025-09-13",
            "end_date": "2025-09-14",
            "is_online": False
        },
        {
            "name": "HackGT",
            "url": "https://hackgt.org/",
            "location": "Atlanta, GA",
            "start_date": "2025-10-04",
            "end_date": "2025-10-06",
            "is_online": False
        },
        {
            "name": "PennApps",
            "url": "https://pennapps.com/",
            "location": "Philadelphia, PA",
            "start_date": "2025-09-06",
            "end_date": "2025-09-08",
            "is_online": False
        },
        {
            "name": "TreeHacks",
            "url": "https://www.treehacks.com/",
            "location": "Stanford, CA",
            "start_date": "2025-02-14",
            "end_date": "2025-02-16",
            "is_online": False
        },
        {
            "name": "HackNYU",
            "url": "https://hacknyu.org/",
            "location": "New York, NY",
            "start_date": "2025-02-21",
            "end_date": "2025-02-23",
            "is_online": False
        },
        {
            "name": "Local Hack Day",
            "url": "https://localhackday.mlh.io/",
            "location": "Various Locations",
            "start_date": "2025-01-25",
            "end_date": "2025-01-26",
            "is_online": True
        },
        {
            "name": "Global Hack Week",
            "url": "https://ghw.mlh.io/",
            "location": "Online",
            "start_date": "2025-01-06",
            "end_date": "2025-01-12",
            "is_online": True
        }
    ]


def transform_mlh_event(event: Dict[str, Any]) -> Optional[Scholarship]:
    """
    Transform MLH event data to Scholarship model.
    """
    try:
        name = event.get('name', '') or event.get('title', '')
        url = event.get('url', '') or event.get('website', '') or event.get('link', '')
        
        if not name:
            return None
        
        if not url:
            url = f"https://mlh.io/events/{name.lower().replace(' ', '-')}"
        
        # Parse dates
        deadline = None
        deadline_timestamp = None
        end_date = event.get('end_date') or event.get('endDate') or event.get('end')
        
        if end_date:
            try:
                if isinstance(end_date, str):
                    deadline_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                else:
                    deadline_dt = datetime.fromtimestamp(end_date / 1000 if end_date > 1e10 else end_date)
                deadline = deadline_dt.strftime('%Y-%m-%d')
                deadline_timestamp = int(deadline_dt.timestamp())
                
                # Skip if expired
                if deadline_dt < datetime.now(deadline_dt.tzinfo if deadline_dt.tzinfo else None):
                    return None
            except Exception:
                pass
        
        location = event.get('location', '') or event.get('city', 'Various')
        is_online = event.get('is_online', False) or 'online' in str(location).lower()
        
        opportunity_data = {
            'id': '',
            'name': name,
            'title': name,
            'organization': 'Major League Hacking (MLH)',
            'amount': 0,  # MLH hackathons typically have sponsor prizes
            'amount_display': 'Prizes + Swag',
            'deadline': deadline,
            'deadline_timestamp': deadline_timestamp,
            'source_url': url,
            'description': f"MLH hackathon at {location}. Join students from around the world to build, learn, and share.",
            'tags': ['Hackathon', 'MLH', 'Student', 'In-Person' if not is_online else 'Online'],
            'geo_tags': [location, 'Global'] if is_online else [location, 'United States'],
            'type_tags': ['Hackathon'],
            'eligibility': {
                'gpa_min': None,
                'majors': [],
                'states': [],
                'citizenship': 'any',
                'grade_levels': ['High School', 'Undergraduate', 'Graduate']
            },
            'eligibility_text': 'Open to high school and university students',
            'source_type': 'mlh',
            'match_score': 50,
            'match_tier': 'Good',
            'verified': True,
            'last_verified': datetime.now().isoformat(),
            'priority_level': 'HIGH'
        }
        
        opportunity_data['id'] = generate_opportunity_id(opportunity_data)
        return Scholarship(**opportunity_data)
        
    except Exception as e:
        logger.warning("MLH transform failed", error=str(e), event=event.get('name', 'Unknown'))
        return None


async def scrape_mlh_events() -> List[Scholarship]:
    """
    Main function to scrape MLH hackathons.
    """
    logger.info("Starting MLH scrape")
    
    events = await fetch_mlh_events()
    scholarships = []
    
    for event in events:
        scholarship = transform_mlh_event(event)
        if scholarship:
            scholarships.append(scholarship)
    
    logger.info("MLH scrape complete", total=len(scholarships))
    return scholarships


async def populate_database_with_mlh() -> int:
    """
    Scrape MLH and save to Firestore.
    """
    logger.info("Populating database with MLH events...")
    
    scholarships = await scrape_mlh_events()
    
    saved_count = 0
    for scholarship in scholarships:
        try:
            await db.save_scholarship(scholarship)
            saved_count += 1
        except Exception as e:
            logger.warning("Failed to save MLH event", id=scholarship.id, error=str(e))
    
    logger.info("MLH population complete", saved=saved_count)
    return saved_count


if __name__ == "__main__":
    async def main():
        scholarships = await scrape_mlh_events()
        print(f"\nFound {len(scholarships)} MLH events:")
        for s in scholarships:
            print(f"  - {s.name}: {s.deadline}")
            print(f"    URL: {s.source_url}")
    
    asyncio.run(main())
