FROM python:3.12-slim

ARG TARGETARCH
ARG SUPERCRONIC_VERSION=v0.2.49
ARG SHA1_AMD64=e63c11a9726b775a6a11801e81af4f3fb926aa68
ARG SHA1_ARM64=0b6c5bb743e0b0dafed1132198c81807927ac413

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && SHA1=$([ "$TARGETARCH" = "arm64" ] && echo "$SHA1_ARM64" || echo "$SHA1_AMD64") \
    && curl -fsSLo /usr/local/bin/supercronic \
       "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-${TARGETARCH}" \
    && echo "${SHA1}  /usr/local/bin/supercronic" | sha1sum -c - \
    && chmod +x /usr/local/bin/supercronic \
    && apt-get purge -y curl && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY src ./src
RUN uv sync --frozen --no-dev
COPY crontab ./crontab

ENV PATH="/app/.venv/bin:$PATH"
CMD ["feedkit", "--help"]
