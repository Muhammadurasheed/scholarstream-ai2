"""
Continuous Devpost Scraper Service
Runs as a long-lived process on Google Cloud Run
Streams hackathon opportunities to Confluent Kafka in real-time
"""
import asyncio
import os
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from app.services.scrapers.devpost_scraper import DevpostScraper
import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
)

logger = structlog.get_logger()


async def continuous_scraping_loop():
    """
    Run Devpost scraper continuously with smart intervals
    
    Strategy:
    - Scrape every 5 minutes for active hackathons (high-value, time-sensitive)
    - Use generator pattern to stream opportunities immediately
    - Implement exponential backoff on errors
    - Flush Kafka producer after each cycle
    """
    scraper = DevpostScraper()
    
    # Enable Kafka streaming
    streaming_enabled = scraper.enable_streaming()
    
    if not streaming_enabled:
        logger.error("Kafka streaming not enabled - check Confluent credentials")
        logger.info("Running in batch mode without streaming")
    
    cycle_count = 0
    error_count = 0
    
    while True:
        cycle_count += 1
        cycle_start = datetime.utcnow()
        
        try:
            logger.info(
                "Starting scraping cycle",
                cycle=cycle_count,
                streaming_enabled=streaming_enabled,
                timestamp=cycle_start.isoformat()
            )
            
            opportunities_found = 0
            
            # Use generator for real-time streaming
            async for opportunity in scraper.scrape_generator():
                opportunities_found += 1
                logger.debug(
                    "Opportunity discovered",
                    name=opportunity.get('name'),
                    organization=opportunity.get('organization'),
                    amount=opportunity.get('amount_display')
                )
            
            # Flush Kafka producer to ensure all messages delivered
            if streaming_enabled:
                scraper.kafka_producer.flush()
            
            cycle_duration = (datetime.utcnow() - cycle_start).total_seconds()
            
            logger.info(
                "Scraping cycle complete",
                cycle=cycle_count,
                opportunities_found=opportunities_found,
                duration_seconds=cycle_duration,
                streaming_enabled=streaming_enabled
            )
            
            # Reset error count on successful cycle
            error_count = 0
            
            # Smart interval: 5 minutes for active sources
            # This balances freshness with API politeness
            await asyncio.sleep(300)  # 5 minutes
            
        except KeyboardInterrupt:
            logger.info("Scraper stopped by user")
            break
            
        except Exception as e:
            error_count += 1
            logger.error(
                "Scraping cycle error",
                cycle=cycle_count,
                error=str(e),
                error_count=error_count,
                exc_info=True
            )
            
            # Exponential backoff on errors (max 5 minutes)
            backoff_seconds = min(60 * (2 ** (error_count - 1)), 300)
            logger.info(f"Backing off for {backoff_seconds} seconds")
            await asyncio.sleep(backoff_seconds)
    
    # Cleanup
    logger.info("Shutting down scraper")
    if streaming_enabled:
        scraper.kafka_producer.close()
    await scraper._close_client()


async def health_check_server():
    """
    Simple HTTP server for Cloud Run health checks
    Runs on port 8080 (Cloud Run default)
    """
    from aiohttp import web
    
    async def health(request):
        return web.json_response({
            "status": "healthy",
            "service": "devpost-scraper",
            "timestamp": datetime.utcnow().isoformat()
        })
    
    app = web.Application()
    app.router.add_get('/health', health)
    app.router.add_get('/', health)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.getenv('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    logger.info(f"Health check server starting on port {port}")
    await site.start()


async def main():
    """
    Main entry point
    Runs health check server and scraping loop concurrently
    """
    logger.info("Starting Devpost Continuous Scraper Service")
    
    # Run health check server and scraping loop concurrently
    await asyncio.gather(
        health_check_server(),
        continuous_scraping_loop()
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service stopped")
    except Exception as e:
        logger.error("Fatal error", error=str(e), exc_info=True)
        sys.exit(1)
