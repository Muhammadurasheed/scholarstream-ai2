import asyncio
import sys
from pathlib import Path
import structlog

# Add backend to path
backend_dir = Path(__file__).parent
sys.path.insert(0, str(backend_dir))

from app.services.scrapers.scraper_registry import ScraperRegistry
from app.services.kafka_config import kafka_producer_manager

logger = structlog.get_logger()

async def test_streaming_scrapers():
    print("================================================================================")
    print("TESTING STREAMING SCRAPERS (Source -> Kafka)")
    print("================================================================================")
    
    registry = ScraperRegistry()
    
    # We'll test with MLH or Fastweb as they are working reliably and fast
    # MLH Scraper
    scraper = registry.scrapers.get("mlh")
    if not scraper:
        print("❌ MLH Scraper not found in registry")
        print(f"Available scrapers: {list(registry.scrapers.keys())}")
        return

    print(f"[*] Testing streaming with {scraper.get_source_name()}...")
    
    # Enable streaming
    if scraper.enable_streaming():
        print("Streaming enabled successfully")
    else:
        print("Failed to enable streaming (check Kafka credentials)")
        return
        
    # Run scrape and stream
    print("Running scrape_and_stream()...")
    opportunities = await scraper.scrape_and_stream()
    
    print(f"Scraped and streamed {len(opportunities)} opportunities")
    
    # Manually flush to be sure (though scrape_and_stream does it)
    kafka_producer_manager.flush(timeout=10)
    print("Producer flushed")
    
    print("\nCheck Confluent Cloud Console to verify messages in 'raw-opportunities-stream'")

if __name__ == "__main__":
    asyncio.run(test_streaming_scrapers())
