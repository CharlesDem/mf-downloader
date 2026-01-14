FROM python:3.12-alpine

WORKDIR /app

COPY file_helper.py .
COPY cron.py .

WORKDIR /app/write

CMD ["python", "-u", "cron.py"]