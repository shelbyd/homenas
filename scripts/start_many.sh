#! /bin/bash

set -euxo pipefail

fusermount -u /tmp/homenas || true
mkdir -p /tmp/homenas || true

RUST_LOG=info cargo run --release -- start /tmp/homenas \
  --backing-dir /tmp/.homenas-store/first \
  --backing-dir /tmp/.homenas-store/second \
  --backing-dir /tmp/.homenas-store/third \
  --backing-dir /tmp/.homenas-store/fourth \
  --backing-dir /tmp/.homenas-store/fifth \
  --backing-dir /tmp/.homenas-store/sixth
