FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install --with-deps chromium
COPY worker.py crawl.py extract.py pipeline.py retrying.py isolation.py isolated_task.py result_store.py memory_recovery.py verify_memory.py verify_recovery.py verify_continuation.py distributed_worker.py shared_queue.py shared_hosts.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "worker.py"]
