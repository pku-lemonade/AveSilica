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

COPY pyproject.toml README.md ./

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && pip install --no-cache-dir --upgrade pip "setuptools>=68" wheel \
    && python -c 'import tomllib; data = tomllib.load(open("pyproject.toml", "rb")); deps = data["project"]["dependencies"] + data["project"]["optional-dependencies"]["codex"]; print("\n".join(deps))' > /tmp/token-zulip-requirements.txt \
    && pip install --no-cache-dir -r /tmp/token-zulip-requirements.txt \
    && rm /tmp/token-zulip-requirements.txt \
    && apt-get purge -y --auto-remove git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=codex-cli /usr/local/bin/node /usr/local/bin/node
COPY --from=codex-cli /usr/local/lib/node_modules/@openai /usr/local/lib/node_modules/@openai
RUN ln -s ../lib/node_modules/@openai/codex/bin/codex.js /usr/local/bin/codex

COPY src ./src
RUN pip install --no-cache-dir --no-deps --no-build-isolation .

COPY workspace ./workspace

ENTRYPOINT ["token-zulip"]
CMD ["run"]
