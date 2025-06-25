FROM python:3.9.7-alpine

# Copy application code
COPY healthcheck /app/healthcheck
COPY protocol /app/protocol
COPY common /app/common
COPY requirements.txt /app/requirements.txt

WORKDIR /app

# Install Python deps (protobuf, pika, etc.)
RUN pip install --no-cache-dir -r requirements.txt

# Add code to PYTHONPATH
ENV PYTHONPATH="/app:/app/common:/app/protocol:${PYTHONPATH}"


RUN apk add --no-cache docker

ENTRYPOINT ["python3", "healthcheck/main.py"]