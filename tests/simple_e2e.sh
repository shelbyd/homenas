#! /bin/bash

set -euo pipefail

cargo build
cargo test

set +e

DIR="/tmp/homenas_a"
./target/debug/homenas start $DIR &
sleep 0.5

ls -lah $DIR

HELLO="$DIR/hello.txt"
TEXT="Hello World!"

echo $TEXT > $HELLO
cat $HELLO
if [[ $(cat $HELLO) != "$TEXT" ]]; then
  echo "Written file does not match expected"
fi

ls -lah $DIR
rm $HELLO
ls -lah $DIR

killall homenas

fusermount -u $DIR

rm -r /tmp/homenas_*
