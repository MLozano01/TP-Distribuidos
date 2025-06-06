FROM python:3.9.7-slim

COPY transformer /
COPY protocol /protocol

COPY transformer/requirements.txt /

RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "/main.py"]