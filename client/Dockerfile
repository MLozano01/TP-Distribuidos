FROM python:3.9.7-slim

COPY client /

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "main.py"]