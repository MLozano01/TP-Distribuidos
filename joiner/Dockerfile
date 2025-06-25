FROM python:3.9.7-slim

# Copy project sources maintaining package structure
COPY joiner /app/joiner
COPY common /app/common
COPY protocol /app/protocol
COPY requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install -r requirements.txt

# Ensure common & protocol packages are importable and create backup dir
ENV PYTHONPATH="/app:/app/joiner:/app/common:/app/protocol:${PYTHONPATH}"
RUN mkdir -p /backup

ENTRYPOINT ["python3", "-m", "joiner.main"]