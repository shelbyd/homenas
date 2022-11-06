#! /bin/bash

set -euo pipefail

cargo test
cargo build

set +e

DIR_A="/tmp/homenas_a"
DIR_B="/tmp/homenas_b"
DIR_C="/tmp/homenas_c"

mkdir $DIR_A || true
mkdir $DIR_B || true
mkdir $DIR_C || true

./target/debug/homenas start $DIR_A \
  --listen-on 42000 --peers=127.0.0.1:42001 --peers=127.0.0.1:42002 &
./target/debug/homenas start $DIR_B \
  --listen-on 42001 --peers=127.0.0.1:42000 --peers=127.0.0.1:42002 &
./target/debug/homenas start $DIR_C \
  --listen-on 42002 --peers=127.0.0.1:42000 --peers=127.0.0.1:42001 &

sleep 2

ls -lah $DIR_A

HELLO="$DIR_A/hello.txt"
TEXT="Hello World!"

echo $TEXT > "$DIR_A/hello.txt"

FROM_B="$(cat "$DIR_B/hello.txt")"
if [[ "$FROM_B" != "$TEXT" ]]; then
  echo "Written file does not match expected from B"
  echo "$FROM_B"
fi

FROM_C="$(cat "$DIR_C/hello.txt")"
if [[ "$FROM_C" != "$TEXT" ]]; then
  echo "Written file does not match expected from C"
  echo "$FROM_C"
fi

killall target/debug/homenas

fusermount -u $DIR_A
fusermount -u $DIR_B
fusermount -u $DIR_C

rm -r /tmp/homenas_*
