"""
TAIKAI Hackathon Scraper
Fetches hackathons from taikai.network using their Next.js data hydration.
"""
import httpx
import json
import re
import structlog
from typing import List, Dict, Any, Optional
from datetime import datetime

from app.services.flink_processor import generate_opportunity_id
from app.database import db
from app.models import Scholarship

logger = structlog.get_logger()

# TAIKAI uses Next.js, so we extract the initial state from __NEXT_DATA__
TAIKAI_URL = "https://taikai.network/hackathons"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
}



from app.services.crawler_service import crawler_service

async def fetch_taikai_events() -> List[Dict[str, Any]]:
    """
    Fetch TAIKAI hackathons by parsing the __NEXT_DATA__ JSON blob.
    Uses Playwright to bypass WAF.
    """
    try:
        logger.info("Fetching TAIKAI via Sentinel Drone...")
        html = await crawler_service.fetch_content(TAIKAI_URL)
        
        if html:
            # Extract JSON blob
            match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html)
            if match:
                data = json.loads(match.group(1))
                
                # Navigate through the props structure to find hackathons
                # Structure usually: props.pageProps.challenges OR props.pageProps.initialState...
                page_props = data.get('props', {}).get('pageProps', {})
                
                # Try direct challenges list
                challenges = page_props.get('challenges', [])
                
                # If empty, try searching in initial state
                if not challenges:
                    initial_state = page_props.get('initialState', {})
                    challenges = initial_state.get('challenges', {}).get('list', [])
                
                # If still empty, try "dehydratedState" (React Query)
                if not challenges:
                    dehydrated = page_props.get('dehydratedState', {})
                    queries = dehydrated.get('queries', [])
                    for q in queries:
                        if 'challenges' in str(query_data := q.get('state', {}).get('data', {})):
                            # Extract from query data
                            if isinstance(query_data, list):
                                challenges = query_data
                            elif isinstance(query_data, dict):
                                challenges = query_data.get('items', []) or query_data.get('challenges', [])
                            if challenges: 
                                break
                
                logger.info("TAIKAI scrape success", count=len(challenges))
                return challenges
                    

    except Exception as e:
        logger.warning("TAIKAI scrape failed", error=str(e))

    # New Logical Path: Parse Apollo State
    try:
        if html and '__NEXT_DATA__' in html:
            match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html)
            if match:
                data = json.loads(match.group(1))
                page_props = data.get('props', {}).get('pageProps', {})
                apollo_state = page_props.get('apolloState', {})
                
                # Extract all objects that look like a Challenge/Hackathon
                challenges = []
                for key, value in apollo_state.items():
                    if key.startswith('Challenge:') and 'name' in value and 'slug' in value:
                        # Ensure it's not a reference but the full object
                        if 'prize' in value or 'shortDescription' in value:
                            challenges.append(value)
                
                if challenges:
                    logger.info("TAIKAI Apollo scrape success", count=len(challenges))
                    return challenges

    except Exception as e:
        logger.warning("TAIKAI Apollo parse failed", error=str(e))
    
    return []


def transform_taikai_event(event: Dict[str, Any]) -> Optional[Scholarship]:
    """
    Transform TAIKAI event data to Scholarship model.
    """
    try:
        title = event.get('name', '') or event.get('title', '')
        slug = event.get('slug', '')
        
        if not title:
            return None
            
        # Construct URL
        organization = event.get('organization', {}).get('slug', 'taikai')
        url = f"https://taikai.network/{organization}/hackathons/{slug}" if slug else TAIKAI_URL
        
        # Parse Prize
        amount = 0
        prize_pool = event.get('prizePool', '0')
        try:
            # Clean string like "$10,000" or "10000 USD"
            clean_prize = str(prize_pool).replace('$', '').replace(',', '').split(' ')[0]
            amount = int(float(clean_prize)) if clean_prize.replace('.', '').isdigit() else 0
        except ValueError:
            pass
            
        amount_display = event.get('prizePool', 'Varies')
        
        # Parse Dates
        deadline = None
        deadline_timestamp = None
        end_date = event.get('endDate') or event.get('registrationDeadline')
        
        if end_date:
            try:
                deadline_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                deadline = deadline_dt.strftime('%Y-%m-%d')
                deadline_timestamp = int(deadline_dt.timestamp())
                
                # Skip expired
                if deadline_dt < datetime.now(deadline_dt.tzinfo):
                    return None
            except:
                pass
        
        # Tags and Metadata
        tags = ['Hackathon', 'TAIKAI']
        tags.extend(event.get('tags', [])[:3])
        
        location = "Online" if event.get('type') == 'Online' else event.get('location', 'Global')
        
        opportunity_data = {
            'id': '',
            'name': title,
            'title': title,
            'organization': event.get('organization', {}).get('name', 'TAIKAI'),
            'amount': amount,
            'amount_display': amount_display,
            'deadline': deadline,
            'deadline_timestamp': deadline_timestamp,
            'source_url': url,
            'description': event.get('description', '')[:500] if event.get('description') else f"Join the {title} hackathon on TAIKAI.",
            'tags': tags,
            'geo_tags': ['Global', 'Online'] if 'Online' in str(location) else [location],
            'type_tags': ['Hackathon'],
            'eligibility': {
                'gpa_min': None,
                'majors': [],
                'states': [],
                'citizenship': 'any',
                'grade_levels': []
            },
            'eligibility_text': 'Open to developers globally',
            'source_type': 'taikai',
            'match_score': 50,
            'match_tier': 'Good',
            'verified': True,
            'last_verified': datetime.now().isoformat(),
            'priority_level': 'HIGH' if amount >= 5000 else 'MEDIUM'
        }
        
        opportunity_data['id'] = generate_opportunity_id(opportunity_data)
        return Scholarship(**opportunity_data)

    except Exception as e:
        logger.warning("TAIKAI transform failed", error=str(e), event=event.get('name', 'Unknown'))
        return None


async def scrape_taikai_events() -> List[Scholarship]:
    """Main scraping function for TAIKAI"""
    logger.info("Starting TAIKAI scrape...")
    events = await fetch_taikai_events()
    
    scholarships = []
    for event in events:
        s = transform_taikai_event(event)
        if s:
            scholarships.append(s)
            
    logger.info("TAIKAI scrape complete", count=len(scholarships))
    return scholarships


async def populate_database_with_taikai() -> int:
    """Scrape and save to DB"""
    scholarships = await scrape_taikai_events()
    count = 0
    for s in scholarships:
        try:
            await db.save_scholarship(s)
            count += 1
        except Exception:
            pass
    return count
