#!/usr/bin/env bash
source ./env  # from env import CREDS


function test-token() {
  python3 sheets.py credentials.json token.json
}


function new-token() {
  rm token.json
  test-token
}


function main() {
  cd tokens

  if ! test-token; then
    new-token
  fi

  cd -
}


main
