#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

### Some general purpose helper functions that can be used in test scripts.

_on_exit_commands=()

function _on_exit_callback {
  # Export the exit code so that it could be used by the callback commands
  export TRAPPED_EXIT_CODE=$?
  # Un-register the callback, to avoid multiple invocations: some shells may treat some signals as subset of others.
  trap "" INT EXIT
  # Fast exit, if there is another keyboard interrupt.
  trap "exit -1" INT

  for command in "${_on_exit_commands[@]-}"; do
    eval "${command}"
  done
}

# Register for multiple signals: some shells interpret them as mutually exclusive.
trap _on_exit_callback INT EXIT

# Helper method to register a command that should be called on current script exit.
# It allows to have multiple "on exit" commands to be called, compared to the built-in `trap "$command" EXIT`.
# Note: tests should not use `trap $command INT|EXIT` directly, to avoid having "Highlander" situation.
function on_exit {
  local command="$1"

  # Keep commands in reverse order, so commands would be executed in LIFO order.
  _on_exit_commands=("${command}" "${_on_exit_commands[@]-}")
}
