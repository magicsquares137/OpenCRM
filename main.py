#!/usr/bin/env python3
"""
Arkitekt OpenCRM - Main Application Entry Point

This is the main entry point for the Arkitekt OpenCRM lead automation pipeline.
It orchestrates the entire workflow: fetching leads from Meta API, storing them in
database, and sending email notifications via Microsoft Graph API.

Features:
- Automatic database schema initialization
- Background health check server
- Continuous polling loop for new leads
- Email sending to intake leads
- Graceful shutdown on signals (SIGTERM/SIGINT)
- Comprehensive error handling with loop continuation
- Startup validation of environment variables
- Processing statistics logging

Workflow:
    1. Load and validate configuration
    2. Initialize database schema
    3. Start health check server (background thread)
    4. Enter main processing loop:
       a. Fetch new leads from Meta API
       b. Insert new leads to database (status='intake')
       c. Query database for leads with status='intake'
       d. Send emails to those leads
       e. Update lead status to 'emailed'
       f. Sleep for configured interval
       g. Repeat
    5. Graceful shutdown on termination signals

Usage:
    # Run directly
    python main.py
    
    # Run in Docker
    docker-compose up -d

Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

import sys
import signal
import time
import logging
from datetime import datetime
from typing import Dict, Any

# Import application modules
from config import config, ConfigurationError
from database import get_database, DatabaseError
from lead_processor import LeadProcessor, LeadProcessorError
from email_sender import EmailSender, EmailSenderError
from health_check import start_health_check_server, stop_health_check_server
from logger import setup_logging, get_logger


# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """
    Handle shutdown signals (SIGTERM, SIGINT).
    
    Args:
        signum: Signal number
        frame: Current stack frame
    """
    global shutdown_requested
    
    signal_name = signal.Signals(signum).name
    logger = logging.getLogger(__name__)
    
    logger.warning(f"Received signal {signal_name} ({signum}), initiating graceful shutdown...")
    shutdown_requested = True


def startup_validation() -> bool:
    """
    Perform startup validation checks.
    
    Validates:
    - Configuration loaded correctly
    - Required environment variables present
    - Database accessible
    
    Returns:
        bool: True if validation passes, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("="*80)
        logger.info("STARTUP VALIDATION")
        logger.info("="*80)
        
        # Validate configuration
        logger.info("Validating configuration...")
        if not config._initialized:
            logger.error("Configuration not initialized")
            return False
        
        logger.info(f"✓ Configuration loaded (Environment: {config.environment})")
        logger.info(f"✓ Database path: {config.db_path}")
        logger.info(f"✓ Poll interval: {config.poll_interval_seconds}s ({config.poll_interval_seconds/60:.1f} min)")
        logger.info(f"✓ Log level: {config.log_level}")
        
        # Test database connection
        logger.info("Testing database connection...")
        db = get_database()
        stats = db.get_stats()
        logger.info(f"✓ Database operational (Total leads: {stats.get('total_leads', 0)})")
        
        # Print startup summary
        logger.info("")
        logger.info("Configuration Summary:")
        logger.info(f"  Meta Page ID:     {config.meta_page_id[:8]}...")
        logger.info(f"  Sender Email:     {config.ms_sender_email}")
        logger.info(f"  Recipients:       {', '.join(config.ms_recipient_emails)}")
        logger.info(f"  Booking URL:      {config.booking_url}")
        logger.info(f"  Health Check:     {'Enabled' if config.health_check_enabled else 'Disabled'}")
        
        logger.info("="*80)
        logger.info("✓ All startup validations passed")
        logger.info("="*80)
        
        return True
        
    except ConfigurationError as e:
        logger.error(f"Configuration validation failed: {e}")
        return False
        
    except DatabaseError as e:
        logger.error(f"Database validation failed: {e}")
        return False
        
    except Exception as e:
        logger.error(f"Unexpected error during startup validation: {e}", exc_info=True)
        return False


def process_pipeline_iteration() -> Dict[str, Any]:
    """
    Execute one iteration of the processing pipeline.
    
    This includes:
    1. Fetch new leads from Meta API
    2. Store new leads in database
    3. Send emails to intake leads
    4. Return statistics
    
    Returns:
        Dictionary with iteration statistics
    """
    logger = logging.getLogger(__name__)
    
    iteration_stats = {
        'timestamp': datetime.utcnow().isoformat(),
        'leads_fetched': 0,
        'leads_inserted': 0,
        'emails_sent': 0,
        'errors': []
    }
    
    try:
        # Step 1: Fetch and process new leads from Meta API
        logger.info("-" * 80)
        logger.info("STEP 1: Fetching leads from Meta API...")
        logger.info("-" * 80)
        
        processor = LeadProcessor()
        processing_stats = processor.process_new_leads()
        
        iteration_stats['leads_fetched'] = processing_stats.get('total_fetched', 0)
        iteration_stats['leads_inserted'] = processing_stats.get('new_leads', 0)
        
        logger.info(f"✓ Fetched {iteration_stats['leads_fetched']} leads")
        logger.info(f"✓ Inserted {iteration_stats['leads_inserted']} new leads")
        logger.info(f"  Skipped {processing_stats.get('duplicates', 0)} duplicates")
        
        # Step 2: Send emails to intake leads
        logger.info("")
        logger.info("-" * 80)
        logger.info("STEP 2: Sending emails to intake leads...")
        logger.info("-" * 80)
        
        sender = EmailSender()
        email_stats = sender.process_intake_leads()
        
        iteration_stats['emails_sent'] = email_stats.get('emails_sent', 0)
        iteration_stats['email_failures'] = email_stats.get('failures', 0)
        
        logger.info(f"✓ Sent {iteration_stats['emails_sent']} emails")
        if iteration_stats['email_failures'] > 0:
            logger.warning(f"⚠ {iteration_stats['email_failures']} email failures")
        
        # Summary
        logger.info("")
        logger.info("-" * 80)
        logger.info("ITERATION SUMMARY")
        logger.info("-" * 80)
        logger.info(f"  Leads Fetched:    {iteration_stats['leads_fetched']}")
        logger.info(f"  Leads Inserted:   {iteration_stats['leads_inserted']}")
        logger.info(f"  Emails Sent:      {iteration_stats['emails_sent']}")
        logger.info(f"  Email Failures:   {iteration_stats.get('email_failures', 0)}")
        logger.info("-" * 80)
        
        return iteration_stats
        
    except LeadProcessorError as e:
        error_msg = f"Lead processing error: {e}"
        logger.error(error_msg, exc_info=True)
        iteration_stats['errors'].append(error_msg)
        return iteration_stats
        
    except EmailSenderError as e:
        error_msg = f"Email sending error: {e}"
        logger.error(error_msg, exc_info=True)
        iteration_stats['errors'].append(error_msg)
        return iteration_stats
        
    except Exception as e:
        error_msg = f"Unexpected error in pipeline iteration: {e}"
        logger.error(error_msg, exc_info=True)
        iteration_stats['errors'].append(error_msg)
        return iteration_stats


def main_loop():
    """
    Main processing loop that runs continuously.
    
    This loop:
    1. Processes one pipeline iteration
    2. Logs statistics
    3. Sleeps for configured interval
    4. Repeats until shutdown requested
    """
    logger = logging.getLogger(__name__)
    
    iteration_count = 0
    total_leads_processed = 0
    total_emails_sent = 0
    
    logger.info("\n" + "="*80)
    logger.info("STARTING MAIN PROCESSING LOOP")
    logger.info("="*80)
    logger.info(f"Poll interval: {config.poll_interval_seconds}s ({config.poll_interval_seconds/60:.1f} minutes)")
    logger.info(f"Press Ctrl+C to stop gracefully")
    logger.info("="*80 + "\n")
    
    while not shutdown_requested:
        try:
            iteration_count += 1
            
            logger.info("")
            logger.info("="*80)
            logger.info(f"ITERATION #{iteration_count} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
            logger.info("="*80)
            
            # Execute pipeline iteration
            stats = process_pipeline_iteration()
            
            # Update totals
            total_leads_processed += stats.get('leads_inserted', 0)
            total_emails_sent += stats.get('emails_sent', 0)
            
            # Log cumulative statistics
            logger.info("")
            logger.info("CUMULATIVE STATISTICS:")
            logger.info(f"  Total Iterations:     {iteration_count}")
            logger.info(f"  Total Leads Inserted: {total_leads_processed}")
            logger.info(f"  Total Emails Sent:    {total_emails_sent}")
            
            # Check for shutdown
            if shutdown_requested:
                logger.info("Shutdown requested, exiting main loop...")
                break
            
            # Sleep until next iteration
            logger.info("")
            logger.info(f"Waiting {config.poll_interval_seconds}s until next iteration...")
            logger.info("="*80 + "\n")
            
            # Sleep with periodic shutdown checks
            sleep_interval = 5  # Check every 5 seconds
            sleep_remaining = config.poll_interval_seconds
            
            while sleep_remaining > 0 and not shutdown_requested:
                time.sleep(min(sleep_interval, sleep_remaining))
                sleep_remaining -= sleep_interval
            
        except KeyboardInterrupt:
            logger.warning("Keyboard interrupt received, initiating shutdown...")
            break
            
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
            logger.warning("Continuing to next iteration after error...")
            
            # Sleep before retrying
            if not shutdown_requested:
                logger.info(f"Waiting {config.poll_interval_seconds}s before retry...")
                time.sleep(config.poll_interval_seconds)
    
    # Shutdown message
    logger.info("\n" + "="*80)
    logger.info("MAIN LOOP TERMINATED")
    logger.info("="*80)
    logger.info(f"Total iterations completed: {iteration_count}")
    logger.info(f"Total leads processed: {total_leads_processed}")
    logger.info(f"Total emails sent: {total_emails_sent}")
    logger.info("="*80 + "\n")


def main():
    """
    Main entry point for the application.
    
    Handles:
    - Configuration loading
    - Logging setup
    - Database initialization
    - Health check server startup
    - Main processing loop
    - Graceful shutdown
    """
    global shutdown_requested
    
    # Print banner
    print("\n" + "="*80)
    print("Arkitekt OpenCRM — Lead Automation Pipeline")
    print("Built by Arkitekt AI")
    print("Version 1.0.0")
    print("="*80 + "\n")
    
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Initialize logging
    try:
        setup_logging()
        logger = get_logger(__name__)
        logger.info("Application starting...")
        
    except Exception as e:
        print(f"[FATAL] Failed to initialize logging: {e}")
        sys.exit(1)
    
    # Load configuration
    try:
        logger.info("Loading configuration...")
        # config is already loaded as singleton, just validate
        if not config._initialized:
            raise ConfigurationError("Configuration not initialized")
        logger.info("✓ Configuration loaded successfully")
        
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Please check your .env file and ensure all required variables are set")
        logger.error("See .env.example for reference")
        sys.exit(1)
    
    # Perform startup validation
    try:
        if not startup_validation():
            logger.error("Startup validation failed, exiting...")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Startup validation error: {e}", exc_info=True)
        sys.exit(1)
    
    # Initialize database (schema creation happens automatically)
    try:
        logger.info("Initializing database...")
        db = get_database()
        logger.info(f"✓ Database initialized: {db.db_path}")
        
    except DatabaseError as e:
        logger.error(f"Database initialization failed: {e}")
        sys.exit(1)
    
    # Start health check server
    health_check_thread = None
    try:
        if config.health_check_enabled:
            logger.info("Starting health check server...")
            health_check_thread = start_health_check_server()
            if health_check_thread:
                logger.info(f"✓ Health check server started on port {config.health_check_port}")
            else:
                logger.warning("Health check server not started (may be disabled or port in use)")
        else:
            logger.info("Health check server disabled in configuration")
            
    except Exception as e:
        logger.warning(f"Failed to start health check server: {e}")
        logger.info("Continuing without health check server...")
    
    # Run main processing loop
    try:
        logger.info("\n" + "="*80)
        logger.info("APPLICATION READY")
        logger.info("="*80 + "\n")
        
        main_loop()
        
    except Exception as e:
        logger.error(f"Fatal error in main loop: {e}", exc_info=True)
        shutdown_requested = True
    
    finally:
        # Graceful shutdown
        logger.info("\n" + "="*80)
        logger.info("SHUTTING DOWN")
        logger.info("="*80)
        
        # Stop health check server
        try:
            if health_check_thread:
                logger.info("Stopping health check server...")
                stop_health_check_server()
                logger.info("✓ Health check server stopped")
        except Exception as e:
            logger.error(f"Error stopping health check server: {e}")
        
        # Close database connections
        try:
            logger.info("Closing database connections...")
            db = get_database()
            db.close()
            logger.info("✓ Database connections closed")
        except Exception as e:
            logger.error(f"Error closing database: {e}")
        
        logger.info("="*80)
        logger.info("APPLICATION TERMINATED")
        logger.info("="*80 + "\n")
    
    # Exit
    sys.exit(0 if not shutdown_requested else 0)


if __name__ == '__main__':
    """
    Script entry point.
    
    Usage:
        python main.py
    """
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[INTERRUPT] Application interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
