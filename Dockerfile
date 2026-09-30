ARG PYTHON_IMAGE=python:3.12-slim
FROM ${PYTHON_IMAGE} AS runtime

RUN useradd --create-home --uid 10001 rtmsf
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/rtmsf ./src/rtmsf
RUN pip install --no-cache-dir '.[aws]'

USER 10001
ENTRYPOINT ["python", "-m", "rtmsf.aws_app"]

FROM runtime AS experiment
COPY src/experiments/__init__.py ./src/experiments/__init__.py
COPY src/experiments/aws/__init__.py src/experiments/aws/crash_worker.py ./src/experiments/aws/
ENV PYTHONPATH=/app/src
ENTRYPOINT ["python", "-m", "experiments.aws.crash_worker"]

FROM runtime AS production
