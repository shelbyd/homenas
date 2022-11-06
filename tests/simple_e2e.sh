#! /bin/bash

set -euo pipefail

DIR="/tmp/homenas_a"

sleep 2
cargo test

ls -lah $DIR

HELLO="$DIR/hello.txt"
TEXT="Hello World!"

echo $TEXT > $HELLO
cat $HELLO
if [[ $(cat $HELLO) != "$TEXT" ]]; then
  echo "Written file does not match expected"

  exit 1
fi
