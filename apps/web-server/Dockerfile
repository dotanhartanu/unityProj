# Web Server API - FastAPI application
# Multi-stage build for smaller final image size

FROM python:3.11-slim AS builder

# Install Python dependencies
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Final stage - minimal runtime image
FROM python:3.11-slim

# Create non-root user for security
RUN useradd --create-home --uid 1000 appuser

WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /root/.local /home/appuser/.local

# Copy application code
COPY --chown=appuser:appuser app.py .

# Switch to non-root user
USER appuser

# Add local pip packages to PATH
ENV PATH=/home/appuser/.local/bin:$PATH

# Application port (documentation only - actual port publishing via Service)
EXPOSE 8000

# Health check for local Docker testing (K8s uses own probes)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Run uvicorn ASGI server
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
