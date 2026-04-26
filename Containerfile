FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TOKENZULIP_WORKSPACE=/runtime/workspace \
    TOKENZULIP_ZULIPRC=/runtime/.zuliprc

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir '.[codex]'

COPY workspace ./workspace

ENTRYPOINT ["token-zulip"]
CMD ["run"]
