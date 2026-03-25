"""
Arkitekt OpenCRM - Health Check Server Module

This module provides a simple HTTP health check endpoint for monitoring
the application's health status. It runs in a separate thread and checks
database connectivity and other critical components.

Features:
- Lightweight HTTP server (without Flask dependency)
- /health endpoint returning JSON status
- / root endpoint with service info
- Database connectivity check
- Thread-safe operation
- Graceful shutdown support
- No external dependencies beyond standard library

Usage:
    from health_check import start_health_check_server, stop_health_check_server
    
    # Start health check server in background thread
    server_thread = start_health_check_server()
    
    # ... run main application ...
    
    # Stop health check server
    stop_health_check_server()

Built by Arkitekt AI — https://arkitekt-ai.com
Version: 1.0.0
"""

import logging
import threading
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any, Optional
from datetime import datetime
import socket

from database import get_database, DatabaseError
from config import config


# Configure module logger
logger = logging.getLogger(__name__)


# Global server instance for shutdown
_server_instance: Optional[HTTPServer] = None
_server_thread: Optional[threading.Thread] = None


class HealthCheckHandler(BaseHTTPRequestHandler):
    """
    HTTP request handler for health check endpoints.
    
    Handles:
    - GET / : Service information
    - GET /health : Health check status with component checks
    """
    
    # Suppress default logging from BaseHTTPRequestHandler
    def log_message(self, format, *args):
        """Override to use application logger instead of stderr."""
        logger.debug(f"Health check request: {format % args}")
    
    def do_GET(self):
        """Handle GET requests."""
        if self.path == '/':
            self._handle_root()
        elif self.path == '/health' or self.path == config.health_check_path:
            self._handle_health_check()
        else:
            self._handle_not_found()
    
    def _handle_root(self):
        """Handle root endpoint - return service information."""
        try:
            response = {
                'service': 'Arkitekt OpenCRM',
                'version': '1.0.0',
                'status': 'running',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'endpoints': {
                    '/': 'Service information',
                    '/health': 'Health check endpoint'
                }
            }
            
            self._send_json_response(200, response)
            
        except Exception as e:
            logger.error(f"Error handling root request: {e}")
            self._send_json_response(500, {'error': 'Internal server error'})
    
    def _handle_health_check(self):
        """Handle health check endpoint - check system components."""
        try:
            # Perform health checks
            health_status = self._check_health()
            
            # Determine HTTP status code
            if health_status['healthy']:
                status_code = 200
            else:
                status_code = 503  # Service Unavailable
            
            self._send_json_response(status_code, health_status)
            
        except Exception as e:
            logger.error(f"Error during health check: {e}")
            error_response = {
                'healthy': False,
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            self._send_json_response(503, error_response)
    
    def _handle_not_found(self):
        """Handle 404 Not Found."""
        response = {
            'error': 'Not Found',
            'message': f'Endpoint {self.path} not found',
            'available_endpoints': ['/', '/health']
        }
        self._send_json_response(404, response)
    
    def _check_health(self) -> Dict[str, Any]:
        """
        Perform health checks on system components.
        
        Returns:
            Dictionary with health status
        """
        checks = {}
        overall_healthy = True
        
        # Check 1: Database connectivity
        db_check = self._check_database()
        checks['database'] = db_check
        if not db_check['healthy']:
            overall_healthy = False
        
        # Check 2: Configuration (basic validation)
        config_check = self._check_configuration()
        checks['configuration'] = config_check
        if not config_check['healthy']:
            overall_healthy = False
        
        # Compile overall health status
        health_status = {
            'healthy': overall_healthy,
            'status': 'healthy' if overall_healthy else 'unhealthy',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'checks': checks,
            'environment': config.environment
        }
        
        return health_status
    
    def _check_database(self) -> Dict[str, Any]:
        """
        Check database connectivity and basic operations.
        
        Returns:
            Dictionary with database health status
        """
        try:
            # Get database instance
            db = get_database()
            
            # Try to get lead count (simple query)
            count = db.get_lead_count()
            
            return {
                'healthy': True,
                'status': 'connected',
                'message': f'Database operational (leads: {count})',
                'database_path': config.db_path
            }
            
        except DatabaseError as e:
            logger.error(f"Database health check failed: {e}")
            return {
                'healthy': False,
                'status': 'error',
                'message': f'Database error: {str(e)}',
                'database_path': config.db_path
            }
            
        except Exception as e:
            logger.error(f"Unexpected error in database health check: {e}")
            return {
                'healthy': False,
                'status': 'error',
                'message': f'Unexpected error: {str(e)}',
                'database_path': config.db_path
            }
    
    def _check_configuration(self) -> Dict[str, Any]:
        """
        Check if critical configuration is present.
        
        Returns:
            Dictionary with configuration health status
        """
        try:
            # Check critical config values
            issues = []
            
            if not config.meta_page_id or config.meta_page_id == 'YOUR_META_PAGE_ID_HERE':
                issues.append('META_PAGE_ID not configured')
            
            if not config.meta_page_access_token or config.meta_page_access_token == 'YOUR_META_PAGE_ACCESS_TOKEN_HERE':
                issues.append('META_PAGE_ACCESS_TOKEN not configured')
            
            if not config.ms_tenant_id or config.ms_tenant_id == 'YOUR_AZURE_TENANT_ID_HERE':
                issues.append('MS_TENANT_ID not configured')
            
            if issues:
                return {
                    'healthy': False,
                    'status': 'misconfigured',
                    'message': 'Configuration issues detected',
                    'issues': issues
                }
            
            return {
                'healthy': True,
                'status': 'valid',
                'message': 'Configuration validated',
                'environment': config.environment
            }
            
        except Exception as e:
            logger.error(f"Configuration health check failed: {e}")
            return {
                'healthy': False,
                'status': 'error',
                'message': f'Configuration check error: {str(e)}'
            }
    
    def _send_json_response(self, status_code: int, data: Dict[str, Any]):
        """
        Send JSON response.
        
        Args:
            status_code: HTTP status code
            data: Dictionary to send as JSON
        """
        try:
            # Convert to JSON
            json_data = json.dumps(data, indent=2)
            
            # Send response
            self.send_response(status_code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(json_data)))
            self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
            self.send_header('Pragma', 'no-cache')
            self.send_header('Expires', '0')
            self.end_headers()
            
            # Write response body
            self.wfile.write(json_data.encode('utf-8'))
            
        except Exception as e:
            logger.error(f"Error sending JSON response: {e}")


def start_health_check_server() -> Optional[threading.Thread]:
    """
    Start health check HTTP server in a background thread.
    
    Returns:
        Thread object running the server, or None if disabled
    """
    global _server_instance, _server_thread
    
    # Check if health check is enabled
    if not config.health_check_enabled:
        logger.info("Health check server is disabled in configuration")
        return None
    
    port = config.health_check_port
    
    try:
        # Create server
        server_address = ('', port)
        _server_instance = HTTPServer(server_address, HealthCheckHandler)
        
        # Start server in background thread
        _server_thread = threading.Thread(
            target=_run_server,
            name='HealthCheckServer',
            daemon=True
        )
        _server_thread.start()
        
        logger.info(f"Health check server started on port {port}")
        logger.info(f"Health check endpoint: http://localhost:{port}/health")
        
        return _server_thread
        
    except socket.error as e:
        if e.errno == 98 or e.errno == 10048:  # Address already in use (Linux/Windows)
            logger.warning(f"Health check port {port} already in use, skipping health check server")
        else:
            logger.error(f"Failed to start health check server: {e}")
        return None
        
    except Exception as e:
        logger.error(f"Failed to start health check server: {e}")
        return None


def _run_server():
    """Run the health check server (called in separate thread)."""
    global _server_instance
    
    try:
        logger.debug("Health check server thread started")
        _server_instance.serve_forever()
    except Exception as e:
        logger.error(f"Health check server error: {e}")
    finally:
        logger.debug("Health check server thread stopped")


def stop_health_check_server():
    """Stop the health check server gracefully."""
    global _server_instance, _server_thread
    
    if _server_instance:
        try:
            logger.info("Stopping health check server...")
            _server_instance.shutdown()
            _server_instance.server_close()
            
            # Wait for thread to finish
            if _server_thread and _server_thread.is_alive():
                _server_thread.join(timeout=5)
            
            _server_instance = None
            _server_thread = None
            
            logger.info("Health check server stopped")
            
        except Exception as e:
            logger.error(f"Error stopping health check server: {e}")


def is_server_running() -> bool:
    """
    Check if health check server is running.
    
    Returns:
        bool: True if running, False otherwise
    """
    return _server_thread is not None and _server_thread.is_alive()


if __name__ == '__main__':
    """
    Test health check server when run as script.
    
    Usage:
        python health_check.py
    """
    # Configure logging for testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    import time
    import requests
    
    print("\n" + "="*80)
    print("Health Check Server - Test")
    print("="*80 + "\n")
    
    try:
        # Start server
        print("[TEST] Starting health check server...")
        thread = start_health_check_server()
        
        if not thread:
            print("[ERROR] Failed to start server")
            exit(1)
        
        print(f"[SUCCESS] Server started on port {config.health_check_port}\n")
        
        # Wait a moment for server to be ready
        time.sleep(1)
        
        # Test root endpoint
        print("[TEST] Testing root endpoint...")
        try:
            response = requests.get(f"http://localhost:{config.health_check_port}/")
            print(f"[RESPONSE] Status: {response.status_code}")
            print(f"[RESPONSE] Body:\n{json.dumps(response.json(), indent=2)}\n")
        except Exception as e:
            print(f"[ERROR] Request failed: {e}\n")
        
        # Test health endpoint
        print("[TEST] Testing health endpoint...")
        try:
            response = requests.get(f"http://localhost:{config.health_check_port}/health")
            print(f"[RESPONSE] Status: {response.status_code}")
            print(f"[RESPONSE] Body:\n{json.dumps(response.json(), indent=2)}\n")
        except Exception as e:
            print(f"[ERROR] Request failed: {e}\n")
        
        # Keep server running for a bit
        print("[INFO] Server running. Press Ctrl+C to stop...\n")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[INFO] Interrupt received, stopping server...")
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop server
        print("\n[TEST] Stopping health check server...")
        stop_health_check_server()
        print("[SUCCESS] Server stopped")
        
        print("\n" + "="*80)
        print("[SUCCESS] Health check tests completed!")
        print("="*80 + "\n")
