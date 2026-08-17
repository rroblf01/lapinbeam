# Build the abi3 wheel with maturin, then install it at runtime.
# For published releases, the runtime stage can simply `pip install lapinbeam`.

FROM rust:1.96-slim AS builder

WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-dev python3-pip python3-venv && \
    pip install --break-system-packages maturin

COPY Cargo.toml Cargo.lock pyproject.toml README.md ./
COPY src ./src
COPY lapinbeam ./lapinbeam

RUN maturin build --release --out /build/dist

FROM python:3.12-slim

WORKDIR /app
COPY --from=builder /build/dist/*.whl .
RUN pip install --no-cache-dir *.whl && rm *.whl

COPY examples/*.py .

CMD ["python", "app_node_a.py"]
