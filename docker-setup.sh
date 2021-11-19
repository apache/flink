#!/bin/bash
# This CI acceptance test is based on:
# https://github.com/jsoref/spelling/tree/04648bdc63723e5cdf5cbeaff2225a462807abc8
# It is conceptually `f` which runs `w` (spelling-unknown-word-splitter)
# plus `fchurn` which uses `dn` mostly rolled together.
set -e

spellchecker='/app'
temp='/tmp/spelling'
dict="$spellchecker/words"

wordlist=https://github.com/check-spelling/check-spelling/raw/dictionary/dict.txt

mkdir -p "$temp"
if [ ! -e "$dict" ]; then
  echo "Retrieving cached $(basename "$wordlist")"
  # english.words is taken from rpm:
  # https://rpmfind.net/linux/fedora/linux/development/rawhide/Everything/aarch64/os/Packages/w/"
  # "words-.*.noarch.rpm"
  (
    curl -L -s "$wordlist" -o "$dict"
  ) >/dev/null 2>/dev/null
fi
