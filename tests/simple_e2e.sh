#! /bin/bash

set -euo pipefail

cargo build
cargo test

set +e

DIR="/tmp/homenas_a"
./target/debug/homenas start $DIR &
sleep 0.5

tree -s -h $DIR

HELLO="$DIR/hello.txt"
TEXT="Hello World!"

echo $TEXT > $HELLO
cat $HELLO
if [[ $(cat $HELLO) != "$TEXT" ]]; then
  echo "Written file does not match expected"
fi

tree -s -h $DIR
rm $HELLO
tree -s -h $DIR

mkdir -p "$DIR/foo/bar/baz"
tree -s -h $DIR
echo $TEXT > "$DIR/foo/bar/baz/hello.txt"

rm -r "$DIR/foo"
tree -s -h $DIR

killall homenas

fusermount -u $DIR

rm -r /tmp/homenas_*
