FROM python:3.12-alpine

WORKDIR /app

COPY ./downloader .
COPY ./common ./common

RUN apk add --no-cache ca-certificates
RUN ls
RUN pip install -r requirements.txt

CMD ["python", "-u", "cron.py"]