#!/usr/bin/env sh
set -eu

IMAGE="${SITAS_LINUX_IMAGE:-rust:latest}"
PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
TARGET_DIR="${SITAS_LINUX_TARGET_DIR:-/tmp/sitas-target}"
DOCKER_IO_URING="${SITAS_DOCKER_IO_URING:-0}"
DOCKER_PRIVILEGED="${SITAS_DOCKER_PRIVILEGED:-0}"
REQUIRE_IO_URING="${SITAS_REQUIRE_IO_URING:-$DOCKER_IO_URING}"

if [ "${DOCKER_IO_URING}" = "0" ] && [ "${DOCKER_PRIVILEGED}" = "0" ]; then
    printf '%s\n' '---' >&2
    printf '%s\n' 'Note: io_uring tests will be skipped because the Docker container blocks' >&2
    printf '%s\n' 'io_uring_setup(2) by default. Set SITAS_DOCKER_IO_URING=1 to unblock it.' >&2
    printf '%s\n' '---' >&2
fi

if [ "$#" -eq 0 ]; then
    set -- sh -lc '
        set -eu
        export PATH="/usr/local/cargo/bin:$PATH"
        rustup component add rustfmt
        rustup component add clippy
        rustup target add aarch64-unknown-none
        cargo fmt --check
        cargo clippy --workspace --all-targets -- -D warnings
        cargo clippy -p sitas-core --features std --all-targets -- -D warnings
        cargo test
        cargo test -p sitas-core --features std
        cargo check -p sitas-core -p sitas-charlotte --target aarch64-unknown-none
        cargo doc --no-deps
        cargo run -p sitas --example basic_kv
        cargo run -p sitas --example concurrent_kv
        cargo run -p sitas --example submit_kv
        cargo run -p sitas --example async_kv
        cargo run -p sitas --example sharded_executor
        cargo run -p sitas --example sharded_index_build
        cargo run -p sitas --example sharded_index_build_uring
        cargo run -p sitas --example sharded_index_mailbox
        cargo run -p sitas --example sharded_observability
        cargo run -p sitas --example sharded_submit
        cargo run -p sitas --example sharded_broadcast
        cargo run -p sitas --example sharded_map_reduce
        cargo run -p sitas --example sharded_cpu_placement
        cargo run -p sitas --example sharded_memory_placement
        cargo run -p sitas --example shard_local
        cargo run -p sitas --example shard_local_handle
        cargo run -p sitas --example shard_local_current
        cargo run -p sitas --example shard_local_workers
        cargo run -p sitas --example shard_local_stoppable_workers
        cargo run -p sitas --example shard_local_stoppable_workers_timeout
        cargo run -p sitas --example shard_local_worker_observability
        cargo run -p sitas --example async_accept
        cargo run -p sitas --example async_connect
        cargo run -p sitas --example async_tcp_echo
        cargo run -p sitas --example async_tcp_pair
        cargo run -p sitas --example async_tcp_server
        cargo run -p sitas --example async_tcp_server_timeout
        cargo run -p sitas --example async_tcp_idle_server
        cargo run -p sitas --example async_tcp_idle_server_timeout
        cargo run -p sitas --example async_tcp_stoppable_server
        cargo run -p sitas --example async_tcp_scoped_server
        cargo run -p sitas --example async_tcp_scheduling_groups
        cargo run -p sitas --example async_tcp_timeout
        cargo run -p sitas --example async_tcp_multi_echo
        cargo run -p sitas --example async_copy
        cargo run -p sitas --example async_readable
        cargo run -p sitas --example async_write
        cargo run -p sitas --example executor_sleep
        cargo run -p sitas --example executor_abort
        cargo run -p sitas --example executor_timeout
        cargo run -p sitas --example executor_race
        cargo run -p sitas --example executor_task_scope
        cargo run -p sitas --example custom_placement
        cargo run -p sitas --example basic_counter
        cargo run -p sitas --example os_reactor
        cargo run -p sitas --example os_readable
        cargo run -p sitas --example os_uring
        cargo run -p sitas --example os_uring_batch
        cargo run -p sitas --example os_uring_abandon
        cargo run -p sitas --example os_uring_lifecycle
        cargo run -p sitas --example scheduling_group_demo
        cargo run -p sitas --example sharded_scheduling_groups
    '
else
    INSTALL_RUSTFMT=0
    INSTALL_CLIPPY=0
    if [ "$1" = "cargo" ] && [ "$#" -ge 2 ]; then
        case "$2" in
            fmt)
                INSTALL_RUSTFMT=1
                ;;
            clippy)
                INSTALL_CLIPPY=1
                ;;
        esac
    fi
    set -- sh -lc '
        set -eu
        export PATH="/usr/local/cargo/bin:$PATH"
        if [ "${SITAS_INSTALL_RUSTFMT:-0}" = "1" ]; then
            rustup component add rustfmt
        fi
        if [ "${SITAS_INSTALL_CLIPPY:-0}" = "1" ]; then
            rustup component add clippy
        fi
        exec "$@"
    ' sh "$@"
fi

if [ "$DOCKER_PRIVILEGED" = "1" ]; then
    exec docker run --rm \
        --privileged \
        -v "$PROJECT_DIR:/work" \
        -w /work \
        -e "CARGO_TARGET_DIR=$TARGET_DIR" \
        -e "SITAS_REQUIRE_IO_URING=$REQUIRE_IO_URING" \
        -e "SITAS_INSTALL_RUSTFMT=${INSTALL_RUSTFMT:-0}" \
        -e "SITAS_INSTALL_CLIPPY=${INSTALL_CLIPPY:-0}" \
        "$IMAGE" \
        "$@"
fi

if [ "$DOCKER_IO_URING" = "1" ]; then
    exec docker run --rm \
        --security-opt seccomp=unconfined \
        -v "$PROJECT_DIR:/work" \
        -w /work \
        -e "CARGO_TARGET_DIR=$TARGET_DIR" \
        -e "SITAS_REQUIRE_IO_URING=$REQUIRE_IO_URING" \
        -e "SITAS_INSTALL_RUSTFMT=${INSTALL_RUSTFMT:-0}" \
        -e "SITAS_INSTALL_CLIPPY=${INSTALL_CLIPPY:-0}" \
        "$IMAGE" \
        "$@"
fi

exec docker run --rm \
    -v "$PROJECT_DIR:/work" \
    -w /work \
    -e "CARGO_TARGET_DIR=$TARGET_DIR" \
    -e "SITAS_REQUIRE_IO_URING=$REQUIRE_IO_URING" \
    -e "SITAS_INSTALL_RUSTFMT=${INSTALL_RUSTFMT:-0}" \
    -e "SITAS_INSTALL_CLIPPY=${INSTALL_CLIPPY:-0}" \
    "$IMAGE" \
    "$@"
