FROM python:3.9.7-slim

COPY filter /
COPY protocol /protocol
COPY common /common


COPY requirements.txt /

RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "/main.py"]