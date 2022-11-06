#! /bin/bash

set -euxo pipefail

DIR="/tmp/homenas_a"
HELLO="$DIR/hello.txt"

sleep 2
cargo build

TEXT="Hello World!"

echo $TEXT > $HELLO
if [[ $(< $HELLO) != "$TEXT" ]]; then
  echo "Written file does not match expected"

  exit 1
fi
