FROM python:3.9.7-slim

COPY data_controller /
COPY protocol /protocol
COPY requirements.txt /
COPY common /common


RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "/main.py"]