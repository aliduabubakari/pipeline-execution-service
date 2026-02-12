FROM python:3.11-slim

WORKDIR /app

COPY execution_api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY execution_api/ /app/execution_api/

ENV PYTHONPATH=/app

EXPOSE 8090
CMD ["python", "-m", "uvicorn", "execution_api.app:app", "--host", "0.0.0.0", "--port", "8090"]