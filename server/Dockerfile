FROM python:3.9.7-slim

COPY server /
COPY protocol /protocol
COPY common /common

COPY requirements.txt /

RUN pip install -r requirements.txt

ENV PYTHONPATH="/common:${PYTHONPATH}"
RUN mkdir -p /backup

ENTRYPOINT ["python3", "/main.py"]