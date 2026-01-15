FROM python:3.12-alpine

WORKDIR /app

COPY file_helper.py .
COPY cron.py .
COPY requirements.txt .
COPY discord_alerter.py .
COPY config.py .

RUN apk add --no-cache ca-certificates

RUN pip install -r requirements.txt

CMD ["python", "-u", "cron.py"]