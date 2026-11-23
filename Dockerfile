# Imagen de la aplicación (productor, pipeline y consumidor): Python 3.12 + Java para KafkaIO.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    HOME=/app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project

COPY src ./src
RUN uv sync --frozen

# Descarga una sola vez el JAR del expansion service de KafkaIO (Java).
RUN /app/.venv/bin/python -c "from apache_beam.io.kafka import default_io_expansion_service as s; x=s(); x.__enter__(); x.__exit__(None, None, None)"

ENV PATH=/app/.venv/bin:$PATH
CMD ["python", "-m", "flota.pipeline"]
