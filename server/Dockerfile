FROM python:3.9.7-slim

COPY server /

# Install dependencies 
RUN apt update && apt install python3 python3-pip -y

ENTRYPOINT ["python3", "/main.py"]