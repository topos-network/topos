#!/bin/bash

set -e

param=$1

function check {
  if [ "$param" == "generate" ]; then
    cargo readme -r $1 > $1/README.md
  else
    diff <(cargo readme  -r $1) $1/README.md || (echo 1>&2 "Please update the $1/README with "'`'"cargo readme -r $1 > $1/README.md"'`' && exit 1 )
  fi
}

check crates/topos-tce-broadcast
check crates/topos-tce-storage
check crates/topos-tce
