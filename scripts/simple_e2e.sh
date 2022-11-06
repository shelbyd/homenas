#! /bin/bash

set -euxo pipefail

DIR="/tmp/homenas_a"

sleep 2
cargo build

ls -lah $DIR

HELLO="$DIR/hello.txt"
TEXT="Hello World!"

echo $TEXT > $HELLO
if [[ $(< $HELLO) != "$TEXT" ]]; then
  echo "Written file does not match expected"

  exit 1
fi
