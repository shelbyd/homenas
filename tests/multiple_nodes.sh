#! /bin/bash

set -euo pipefail

cargo test
cargo build

set +e

killall homenas

DIR_A="/tmp/homenas_a"
DIR_B="/tmp/homenas_b"
DIR_C="/tmp/homenas_c"

rm -r $DIR_A; mkdir $DIR_A
rm -r $DIR_B; mkdir $DIR_B
rm -r $DIR_C; mkdir $DIR_C

sleep 0.1

./target/debug/homenas start $DIR_A \
  --listen-on 42000 --peers=127.0.0.1:42001 --peers=127.0.0.1:42002 &
NODE_A=$!
./target/debug/homenas start $DIR_B \
  --listen-on 42001 --peers=127.0.0.1:42000 --peers=127.0.0.1:42002 &

sleep 1

HELLO="$DIR_A/hello.txt"
TEXT="Hello World!"

echo $TEXT > "$DIR_A/hello.txt"

sleep 0.5

# DIR_C starts after the file has already been written.
./target/debug/homenas start $DIR_C \
  --listen-on 42002 --peers=127.0.0.1:42000 --peers=127.0.0.1:42001 &

sleep 0.5

echo "Testing A"
ls -lah $DIR_A
FROM_A="$(cat "$DIR_A/hello.txt")"
if [[ "$FROM_A" != "$TEXT" ]]; then
  echo "Written file does not match expected from A"
  echo "$FROM_A"
fi

kill $NODE_A

echo "Testing B"
ls -lah $DIR_B
FROM_B="$(cat "$DIR_B/hello.txt")"
if [[ "$FROM_B" != "$TEXT" ]]; then
  echo "Written file does not match expected from B"
  echo "$FROM_B"
fi

echo "Testing C"
ls -lah $DIR_B
FROM_C="$(cat "$DIR_C/hello.txt")"
if [[ "$FROM_C" != "$TEXT" ]]; then
  echo "Written file does not match expected from C"
  echo "$FROM_C"
fi

killall homenas

fusermount -u $DIR_A
fusermount -u $DIR_B
fusermount -u $DIR_C

rm -r /tmp/homenas_*
