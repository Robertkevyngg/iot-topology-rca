FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY topology ./topology

RUN pip install --upgrade pip && pip install .

EXPOSE 8000

CMD ["python", "-m", "iot_rca.api"]
