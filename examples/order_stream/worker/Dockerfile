FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev

COPY main.py db.py order.py ./

CMD ["uv", "run", "--no-sync", "python", "main.py"]
