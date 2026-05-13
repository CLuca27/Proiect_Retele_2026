FROM python:3.13-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

COPY app ./app

CMD ["python", "-m", "app.main"]
