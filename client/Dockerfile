FROM python:3.9.7-slim

COPY client /
COPY protocol /protocol
COPY requirements.txt /

# Install dependencies 
# RUN apt update && apt install python3 python3-pip -y
RUN pip install -r requirements.txt

ENTRYPOINT ["/bin/sh"]