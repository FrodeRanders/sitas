#!/usr/bin/env sh
set -eu

IMAGE="${SITAS_LINUX_IMAGE:-rust:latest}"
PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
TARGET_DIR="${SITAS_LINUX_TARGET_DIR:-/tmp/sitas-target}"

if [ "$#" -eq 0 ]; then
    set -- sh -lc '
        set -eu
        export PATH="/usr/local/cargo/bin:$PATH"
        rustup component add rustfmt
        cargo fmt --check
        cargo test
        cargo doc --no-deps
        cargo run --example basic_kv
        cargo run --example concurrent_kv
        cargo run --example submit_kv
        cargo run --example async_kv
        cargo run --example async_accept
        cargo run --example async_connect
        cargo run --example async_tcp_echo
        cargo run --example async_tcp_pair
        cargo run --example async_tcp_timeout
        cargo run --example async_tcp_multi_echo
        cargo run --example async_copy
        cargo run --example async_readable
        cargo run --example async_write
        cargo run --example executor_sleep
        cargo run --example executor_timeout
        cargo run --example custom_placement
        cargo run --example basic_counter
        cargo run --example os_reactor
        cargo run --example os_readable
    '
fi

exec docker run --rm \
    -v "$PROJECT_DIR:/work" \
    -w /work \
    -e "CARGO_TARGET_DIR=$TARGET_DIR" \
    "$IMAGE" \
    "$@"
