FROM python:3.12-alpine

WORKDIR /app

COPY file_helper.py .
COPY cron.py .
COPY requirements.txt .
COPY minio_config.py .
COPY discord_alerter.py .

RUN mkdir -p compressed

RUN pip install -r requirements.txt

CMD ["python", "-u", "cron.py"]