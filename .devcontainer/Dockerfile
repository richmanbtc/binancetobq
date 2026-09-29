FROM golang:1.26-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git jq procps ripgrep tmux \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
RUN case "$TARGETARCH" in \
        amd64) arch=x86_64 ;; \
        arm64) arch=aarch64 ;; \
        *) exit 1 ;; \
    esac \
    && curl -fsSL "https://github.com/openai/codex/releases/latest/download/codex-${arch}-unknown-linux-musl.tar.gz" \
        | tar -xz -C /usr/local/bin \
    && mv "/usr/local/bin/codex-${arch}-unknown-linux-musl" /usr/local/bin/codex \
    && mkdir -p /root/.codex \
    && codex --version
