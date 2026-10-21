FROM python:3.11-slim-bookworm@sha256:f06d01d4ba3f8ad5af45a3ec87df19c1d317d8e8961edc0fabda61811af6f7cf
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"
WORKDIR /workspace
RUN pip install --no-cache-dir uv==0.9.5
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project
COPY scripts/fetch_jars.py scripts/fetch_jars.py
RUN python scripts/fetch_jars.py
COPY . .
RUN uv sync --frozen
CMD ["urban-geo", "--help"]
