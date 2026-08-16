# Runtime image for lapinbeam applications.
# The wheel is installed from PyPI: no Rust toolchain is needed at runtime.

FROM python:3.12-slim

WORKDIR /app

# Install the published package (uv-uploaded) plus the example apps.
RUN pip install --no-cache-dir lapinbeam

COPY examples/app_node_a.py examples/app_node_b.py .

CMD ["python", "app_node_a.py"]
