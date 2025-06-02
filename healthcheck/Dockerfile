FROM python:3.9.7-alpine

COPY healthcheck /
COPY utils /utils

RUN apk update
RUN apk add docker

ENTRYPOINT ["python3", "/main.py"]