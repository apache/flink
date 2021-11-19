#!/bin/bash
if [ "$INITIALIZED" != defined ]; then
  if [ "$RUNNER_OS" = "Windows" ]; then
    echo "::error ::Windows isn't currently supported"
    exit 5
  fi

  now() {
    date +'%s%N'
  }
  start=$(now)
  temp="${temp:-/tmp/spelling}"
  mkdir -p $temp
  export temp
  if [ -n "$DEBUG" ]; then
    set -x
    begin_group() {
      echo "::group::$1"
    }
    end_group() {
      echo '::end_group::'
    }
  else
    begin_group() {
      echo "(...$1...)"
    }
    end_group() {
      :
    }
    INITIALIZED=defined
  fi
fi
