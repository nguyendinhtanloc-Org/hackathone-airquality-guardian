# Dockerfile - Multi-stage build để tối ưu size
FROM python:3.11-slim AS builder

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --break-system-packages

# Final stage
FROM python:3.11-slim

WORKDIR /app

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Copy only necessary files
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY src/ ./src/
COPY .env.example ./

# Create logs directory
RUN mkdir -p logs

# Healthcheck placeholder - sẽ cập nhật sau khi có API endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

CMD ["python", "-m", "src.main"]
