# Arkitekt OpenCRM - Dockerfile
# ===========================================
# Multi-stage Docker build for production-ready containerization
# 
# Features:
# - Multi-stage build for optimized image size
# - Python 3.12 slim base for security and size
# - Non-root user for enhanced security
# - Health check endpoint
# - Proper signal handling
# - Volume mounts for persistent data
# - Comprehensive logging

# Stage 1: Builder Stage
# ----------------------
# Build dependencies and prepare the application
FROM python:3.12-slim AS builder

# Set build-time metadata
LABEL stage=builder
LABEL description="Builder stage for Arkitekt OpenCRM"

# Set environment variables for Python
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create and set working directory
WORKDIR /build

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies in a virtual environment
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --upgrade pip setuptools wheel && \
    /opt/venv/bin/pip install -r requirements.txt

# Stage 2: Runtime Stage
# ----------------------
# Create minimal runtime image
FROM python:3.12-slim AS runtime

# Set runtime metadata
LABEL maintainer="Arkitekt AI <https://arkitekt-ai.com>"
LABEL version="1.0.0"
LABEL description="Arkitekt OpenCRM — Lead Automation Pipeline by Arkitekt AI"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    APP_HOME=/app \
    DATA_DIR=/app/data \
    LOGS_DIR=/app/logs

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Required for SSL/TLS connections
    ca-certificates \
    # Timezone support
    tzdata \
    # Process management
    tini \
    # Health check HTTP client
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Create non-root user and group for security
# UID/GID 1000 is commonly used and avoids conflicts
RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid 1000 --shell /bin/bash --create-home appuser

# Create application directory structure
RUN mkdir -p ${APP_HOME} ${DATA_DIR} ${LOGS_DIR} && \
    chown -R appuser:appuser ${APP_HOME}

# Set working directory
WORKDIR ${APP_HOME}

# Copy application code (all Python files are in root)
COPY --chown=appuser:appuser *.py ./
COPY --chown=appuser:appuser scripts/ ./scripts/

# Copy example configuration (will be overridden by volume mounts or env vars)
COPY --chown=appuser:appuser ./.env.example ./.env.example

# Create necessary subdirectories
RUN mkdir -p \
    ${DATA_DIR}/database \
    ${DATA_DIR}/cache \
    ${LOGS_DIR}/application \
    ${LOGS_DIR}/system \
    && chown -R appuser:appuser ${DATA_DIR} ${LOGS_DIR}

# Switch to non-root user
USER appuser

# Expose health check port (if health check endpoint is implemented)
# Note: The application should implement a simple HTTP health check server
EXPOSE 8080

# Define volumes for persistent data
# These should be mounted when running the container
VOLUME ["${DATA_DIR}", "${LOGS_DIR}"]

# Health check configuration
# Checks if the process is running and responsive every 30 seconds
# Note: Requires health_check.py or HTTP endpoint implementation
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Set tini as entrypoint for proper signal handling
# This ensures graceful shutdown and proper process management
ENTRYPOINT ["/usr/bin/tini", "--"]

# Default command to run the application
# Can be overridden at runtime with docker run ... <command>
CMD ["python", "main.py"]

# Docker Build Instructions
# =========================
# 
# Build the image:
# docker build -t opencrm:latest .
#
# Build with specific version tag:
# docker build -t opencrm:1.0.0 .
#
# Build with build arguments (if needed):
# docker build --build-arg PYTHON_VERSION=3.12 -t opencrm:latest .
#
# Docker Run Instructions
# =======================
#
# Run with environment variables from file:
# docker run -d \
#   --name meta-leads-automation \
#   --env-file .env \
#   -v $(pwd)/data:/app/data \
#   -v $(pwd)/logs:/app/logs \
#   --restart unless-stopped \
#   opencrm:latest
#
# Run with explicit environment variables:
# docker run -d \
#   --name meta-leads-automation \
#   -e META_ACCESS_TOKEN=your_token \
#   -e META_FORM_ID=your_form_id \
#   -e AZURE_CLIENT_ID=your_client_id \
#   -e AZURE_CLIENT_SECRET=your_secret \
#   -e AZURE_TENANT_ID=your_tenant \
#   -e SENDER_EMAIL=sender@example.com \
#   -e RECIPIENT_EMAIL=recipient@example.com \
#   -v $(pwd)/data:/app/data \
#   -v $(pwd)/logs:/app/logs \
#   --restart unless-stopped \
#   opencrm:latest
#
# Run interactively for debugging:
# docker run -it --rm \
#   --env-file .env \
#   -v $(pwd)/data:/app/data \
#   -v $(pwd)/logs:/app/logs \
#   opencrm:latest \
#   /bin/bash
#
# Docker Compose Usage
# ====================
#
# See docker-compose.yml for orchestration configuration
# Start services: docker-compose up -d
# Stop services: docker-compose down
# View logs: docker-compose logs -f
#
# Security Considerations
# =======================
#
# 1. Never include .env file in the image
# 2. Always use non-root user (implemented)
# 3. Use secrets management for production (Docker Secrets, Kubernetes Secrets)
# 4. Regularly update base image for security patches
# 5. Scan image for vulnerabilities: docker scan opencrm:latest
# 6. Use read-only root filesystem if possible: docker run --read-only ...
#
# Production Recommendations
# ==========================
#
# 1. Use specific version tags instead of 'latest'
# 2. Implement proper secrets management
# 3. Configure resource limits (CPU, memory)
# 4. Set up monitoring and alerting
# 5. Use orchestration platform (Kubernetes, Docker Swarm)
# 6. Implement backup strategy for data volume
# 7. Configure log rotation and aggregation
# 8. Use multi-replica deployment for high availability
