#!/bin/bash

# Check if there are any packages foget to add `TestingT` when use "github.com/pingcap/check".

res=$(diff <(grep -rl --include=\*_test.go "github.com/pingcap/check" | xargs dirname | sort -u) \
     <(grep -rl --include=\*_test.go -E "^\s*(check\.)?TestingT\(" | xargs dirname | sort -u))

if [ "$res" ]; then
  echo "following packages may be lost TestingT:"
  echo "$res" | awk '{if(NF>1){print $2}}'
  exit 1
fi

exit 0
