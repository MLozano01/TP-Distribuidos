FROM python:3.9.7-slim

COPY data_controller /
COPY common /common
COPY protocol /protocol
COPY requirements.txt /
COPY common /common


RUN pip install -r requirements.txt

ENV PYTHONPATH="/common:${PYTHONPATH}"
RUN mkdir -p /backup

ENTRYPOINT ["python3", "/main.py"]