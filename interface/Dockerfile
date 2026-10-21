FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY interface/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY interface /app/interface
COPY fraud_detector/__init__.py fraud_detector/preprocessing.py fraud_detector/scorer.py /app/fraud_detector/

CMD ["streamlit", "run", "interface/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
