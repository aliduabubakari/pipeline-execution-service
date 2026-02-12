FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    requests pandas numpy psutil aiohttp codecarbon prometheus-client \
 && pip install --no-cache-dir --upgrade git+https://github.com/aliduabubakari/semtui1.1.git

COPY runner/run_with_carbon.py /app/run_with_carbon.py

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/run_with_carbon.py"]