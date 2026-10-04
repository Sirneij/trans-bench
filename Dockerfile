# The public, read-only Web UI of trans-bench (railway.json deploys it on Railway).
#
#   docker build -t trans-bench-ui .
#   docker run --rm -p 10000:10000 trans-bench-ui      # then open http://localhost:10000
#
# The image holds the descriptors, the rule files and the published campaigns under results/, and
# no database: read-only mode refuses every view that would start a benchmark or change a file.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TRANS_BENCH_READ_ONLY=1 \
    PORT=10000

WORKDIR /app
COPY requirements_web.txt .
RUN pip install --no-cache-dir -r requirements_web.txt

COPY . .
RUN useradd --create-home --uid 10001 web && chown -R web /app
USER web

EXPOSE 10000
# one process, so that every page sees the same state; threads serve the requests
CMD gunicorn --bind "0.0.0.0:${PORT}" --worker-class gthread --workers 1 --threads 8 --timeout 120 \
    --access-logfile - "ui.app:create_app()"
