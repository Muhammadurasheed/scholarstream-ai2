
import httpx
import asyncio
import structlog
from typing import List, Dict, Any, Optional
import time

from app.services.kafka_config import KafkaConfig, kafka_producer_manager

logger = structlog.get_logger()


from playwright.async_api import async_playwright, BrowserContext, Page
import random

class UniversalCrawlerService:
    """
    Universal Crawler Service (Hunter Drones)
    Powered by Playwright for stealth, JS-execution, and dynamic interactions.
    """
    
    def __init__(self):
        self.kafka_initialized = kafka_producer_manager.initialize()
        self.browser = None
        self.playwright = None
        
    async def _init_browser(self):
        """Initialize Playwright Engine if not running"""
        if not self.playwright:
            self.playwright = await async_playwright().start()
            # Launch in Headless mode (but defined as non-headless to anti-bots)
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-infobars',
                    '--window-size=1920,1080',
                ]
            )
            
    async def create_stealth_context(self) -> BrowserContext:
        """Create a new incognito context with advanced stealth overrides"""
        await self._init_browser()
        
        # Rotate user agents for anti-detection
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        
        # Randomize viewport slightly for fingerprint variance
        viewports = [
            {'width': 1920, 'height': 1080},
            {'width': 1536, 'height': 864},
            {'width': 1440, 'height': 900},
            {'width': 1366, 'height': 768},
        ]
        
        context = await self.browser.new_context(
            user_agent=random.choice(user_agents),
            viewport=random.choice(viewports),
            locale='en-US',
            timezone_id=random.choice(['America/New_York', 'America/Los_Angeles', 'Europe/London']),
            color_scheme='light',
            has_touch=False,
            is_mobile=False,
            java_script_enabled=True,
        )
        
        # Advanced stealth scripts
        await context.add_init_script("""
            // Remove webdriver flag
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            
            // Override plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Override languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            
            // Override visibility state
            Object.defineProperty(document, 'visibilityState', { get: () => 'visible' });
            Object.defineProperty(document, 'hidden', { get: () => false });

            // Override platform
            Object.defineProperty(navigator, 'platform', {
                get: () => 'Win32'
            });
            
            // Override hardware concurrency
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 8
            });
            
            // Override device memory
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => 8
            });
            
            // Remove automation indicators from chrome object
            if (window.chrome) {
                window.chrome.runtime = {};
            }
            
            // Override permissions query
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
        """)
        
        return context

    async def crawl_and_stream(self, urls: List[str], intent: str = "general"):
        """
        Deploy Hunter Drones to target URLs in parallel batches.
        """
        logger.info("Deploying Hunter Drone Squad", target_count=len(urls), intent=intent)
        
        # Process in batches to not overwhelm local resources but keep speed
        batch_size = 5
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            tasks = [self._crawl_single_target(url, intent) for url in batch]
            await asyncio.gather(*tasks)
            # Stagger between batches
            await asyncio.sleep(random.uniform(2.0, 4.0))

    async def _crawl_single_target(self, url: str, intent: str):
        """Individual drone mission"""
        context = await self.create_stealth_context()
        try:
            # BLOCKED BLACKLIST: Hard stop for dead/zombie URLs
            if "chegg.com" in url or "chegg.com" in url.lower():
                logger.warning("Drone ignoring dead target (Chegg Blacklist)", url=url)
                return

            page = await context.new_page()
            # RADICAL PURGE: Block heavy tracking & social scripts to prevent networkidle hangs
            BLOCKED_DOMAINS = [
                "google-analytics.com", "googletagmanager.com", "facebook.net", 
                "clarity.ms", "hotjar.com", "linkedin.com", "doubleclick.net", 
                "quantserve.com", "scorecardresearch.com", "intercom.io"
            ]
            
            async def _handle_route(route):
                request = route.request
                url = request.url.lower()
                resource_type = request.resource_type
                
                # 1. Block heavy resource types
                if resource_type in ["image", "media", "font"]:
                    return await route.abort()
                
                # 2. Block tracking/social scripts (Exclude mission-critical APIs)
                SAFE_DOMAINS = ["api.", "graphql", "cdn-cgi", "dorahacks.io", "hackquest.io", "superteam.fun", "taikai.network"]
                if any(domain in url for domain in BLOCKED_DOMAINS):
                    # Only block if it's NOT a safe API/data domain
                    if not any(safe in url for safe in SAFE_DOMAINS):
                        return await route.abort()
                
                return await route.continue_()

            await page.route("**/*", _handle_route)

            # SET REALISTIC HEADERS
            await page.set_extra_http_headers({
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1"
            })

            logger.info("Drone approaching target", url=url)
            
            # SMART NAVIGATION
            try:
                # Use domcontentloaded for faster, less brittle navigation
                # Only use networkidle if strictly necessary (it fails on sites with constant polling)
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            except Exception as e:
                logger.debug("Drone primary approach failed (networkidle), retrying with lenient wait", url=url)
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                except:
                    # Last resort: just wait for the request to commit
                    await page.goto(url, wait_until="commit", timeout=60000)
            
            # HUMAN INTERACTION LAYER (The "Wiggle")
            # Move mouse randomly to simulate presence
            await page.mouse.move(random.randint(100, 500), random.randint(100, 500))
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
            # DEEP SCROLL (For Infinite Scroll sites like DoraHacks/DevPost)
            # We scroll in 3 intervals to trigger lazy loads
            for _ in range(3):
                await page.evaluate("window.scrollBy(0, 1500)")
                await asyncio.sleep(1.5)
            
            # Wait for content to stabilize
            try:
                # RADICAL: Added a 2s 'Snap Wait' for SPA hydration stabilization
                await asyncio.sleep(2.0)
                await page.wait_for_load_state("networkidle", timeout=10000)
            except:
                pass 

            # EXTRACT
            content = await page.content()
            title = await page.title()
            
            # CONTENT GUARD: Don't transmit shells or error pages
            if "Page Not Found" in title or "404" in title:
                logger.warning("Drone mission aborted: 404/Not Found", url=url, title=title)
                return
                
            if len(content) < 5000:
                logger.warning("Drone mission aborted: Content too thin (Potential Loading Shell)", url=url, size=len(content))
                return
            
            await self._process_success(url, content, title, intent)
            
        except Exception as e:
            logger.error("Drone crash", url=url, error=str(e))
        finally:
            await context.close()
            
    async def _process_success(self, url: str, html_content: str, title: str, intent: str):
        """Process successful extraction"""
        
        # 1. Clean / Minify HTML (basic) to save bandwidth
        # remove scripts/styles for raw storage if desired, but we keep raw for now
        
        payload = {
            "url": url,
            "title": title,
            "html": html_content[:200000],  # 200KB limit
            "crawled_at": time.time(),
            "source": self._extract_domain(url),
            "intent": intent,
            "agent_type": "HunterDrone-V1"
        }
        
        if self.kafka_initialized:
            success = kafka_producer_manager.publish_to_stream(
                topic=KafkaConfig.TOPIC_RAW_HTML,
                key=url,
                value=payload
            )
            if success:
                logger.info("Drone transmitted payload", url=url, size=len(html_content))
            else:
                logger.error("Transmission jammed (Kafka fail) - Engaging Heartbeat Fallback", url=url)
                from app.services.cortex.refinery import refinery_service
                await refinery_service.process_raw_event(key=url, value=payload) # Direct Heartbeat Injection
        else:
             logger.warning("Kafka offline - Engaging Heartbeat Fallback", url=url)
             from app.services.cortex.refinery import refinery_service
             await refinery_service.process_raw_event(key=url, value=payload) # Direct Heartbeat Injection

    def _extract_domain(self, url: str) -> str:
        from urllib.parse import urlparse
        return urlparse(url).netloc
    
    async def fetch_content(self, url: str) -> Optional[str]:
        """
        Direct fetch of a single URL using stealth context. 
        Returns HTML content or None on failure.
        Useful for specific scrapers that need immediate return values.
        """
        context = await self.create_stealth_context()
        page = None
        try:
            page = await context.new_page()
            # Basic route blocking for speed
            await page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media", "font", "stylesheet"] else route.continue_())
            
            logger.info("Direct fetch approaching", url=url)
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            except:
                logger.warning("Direct fetch timeout, retrying with fast load", url=url)
                await page.goto(url, wait_until="load", timeout=30000)
            
            # Small wiggle/wait
            await asyncio.sleep(2)
            
            content = await page.content()
            return content
            
        except Exception as e:
            logger.error("Direct fetch failed", url=url, error=str(e))
            return None
        finally:
            if page: await page.close()
            await context.close()

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

# Global instance
crawler_service = UniversalCrawlerService()

