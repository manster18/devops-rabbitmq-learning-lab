FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py dlq_handler.py ./

# No ENTRYPOINT/CMD on purpose: this image is only ever run via
# `docker compose run --rm consumer python consumer.py` (see README),
# so the full command is supplied by the caller. An ENTRYPOINT here would
# cause it to be prepended to that command, invoking the script twice.
