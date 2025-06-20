FROM python:3.9.7-slim

COPY reducer /
COPY common /common
COPY protocol /protocol

COPY requirements.txt /

RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "/main.py"]