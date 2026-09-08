# Python 3.12 is used deliberately: confluent-kafka ships prebuilt wheels for
# it, so the image needs no compiler toolchain and builds in seconds.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schemas/ ./schemas/
COPY src/ ./src/

CMD ["python", "-m", "src.consumer"]
