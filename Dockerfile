FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY pylog ./pylog
COPY cli.py bench.py demo.py raft_demo.py ./
COPY tests ./tests

RUN pip install --no-cache-dir -e . pytest

EXPOSE 8787 8788 9711 9712 9713

CMD ["python", "-m", "pytest", "-q"]
