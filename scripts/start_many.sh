#! /bin/bash

set -uxo pipefail

killall homenas || true

mkdir -p /tmp/homenas || true

fusermount -u /tmp/homenas/first || true
fusermount -u /tmp/homenas/second || true
fusermount -u /tmp/homenas/third || true

rm -r /tmp/homenas/*

set -e

RUST_LOG=info cargo run --release -- start /tmp/homenas/first \
  --backing-dir /tmp/homenas/first-store \
  --listen-on 42001 \
  &

RUST_LOG=info cargo run --release -- start /tmp/homenas/second \
  --backing-dir /tmp/homenas/second-store \
  --listen-on 42002 \
  --peers 127.0.0.1:42001 \
  &

RUST_LOG=info cargo run --release -- start /tmp/homenas/third \
  --backing-dir /tmp/homenas/third-store \
  --listen-on 42003 \
  --peers 127.0.0.1:42001 \
  &

wait $(pidof homenas)
