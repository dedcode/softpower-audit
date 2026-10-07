FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install --with-deps chromium
COPY *.py ./
RUN python -c "import worker, distributed_worker, shared_queue, shared_hosts, transaction_retry"
ENV PYTHONUNBUFFERED=1
CMD ["python", "worker.py"]
