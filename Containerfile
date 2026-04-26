FROM node:22-bookworm-slim AS codex-cli

ARG CODEX_CLI_VERSION=latest

RUN npm install -g --omit=dev --no-audit --no-fund "@openai/codex@${CODEX_CLI_VERSION}" \
    && npm cache clean --force

FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TOKENZULIP_WORKSPACE=/runtime/workspace \
    TOKENZULIP_ZULIPRC=/runtime/.zuliprc

WORKDIR /app

COPY --from=codex-cli /usr/local/bin/node /usr/local/bin/node
COPY --from=codex-cli /usr/local/lib/node_modules/@openai /usr/local/lib/node_modules/@openai

RUN ln -s ../lib/node_modules/@openai/codex/bin/codex.js /usr/local/bin/codex

COPY pyproject.toml README.md ./
COPY src ./src

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir '.[codex]' \
    && apt-get purge -y --auto-remove git \
    && rm -rf /var/lib/apt/lists/*

COPY workspace ./workspace

ENTRYPOINT ["token-zulip"]
CMD ["run"]
