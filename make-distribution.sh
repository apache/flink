#!/usr/bin/env bash

set -o pipefail
set -e 
set -x

# make-distribution.sh - tool for making binary distributions of Flink
FLINK_HOME="$(cd "`dirname "$0"`"; pwd)"
VERSION=$(mvn help:evaluate -Dexpression=project.version $@ 2>/dev/null | grep -v "INFO" | tail -n 1)
TARDIR_NAME="flink-$VERSION-bin"
TARDIR="$FLINK_HOME/$TARDIR_NAME"
mkdir -p "$TARDIR"
cp -r $FLINK_HOME/build-target/* "$TARDIR"
tar czf "flink-$VERSION-bin.tgz" -C "$FLINK_HOME" "$TARDIR_NAME" 
rm -rf "$TARDIR"
