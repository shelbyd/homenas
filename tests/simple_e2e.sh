#! /bin/bash

set -euo pipefail

cargo build
cargo test

set +e

DIR="/tmp/homenas_a"
./target/debug/homenas start $DIR \
  --backing-dir /tmp/.homenas-store/main \
  --backing-dir /tmp/.homenas-store/backup \
  &

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
echo $TEXT > "$DIR/foo/bar/baz/hello.txt"
tree -s -h $DIR

rm -r "$DIR/foo"
tree -s -h $DIR

killall homenas

fusermount -u $DIR

rm -r /tmp/homenas_*
