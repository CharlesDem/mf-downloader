FROM python:3.12-alpine

WORKDIR /app

COPY . .

RUN apk add --no-cache ca-certificates

RUN pip install -r requirements.txt

CMD ["python", "-u", "cron.py"]