FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY fraud_detector/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY fraud_detector/__init__.py fraud_detector/app.py fraud_detector/preprocessing.py fraud_detector/scorer.py fraud_detector/score_writer.py /app/fraud_detector/

CMD ["python", "-m", "fraud_detector.app"]
