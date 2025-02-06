################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import re
import sys

# force to register the operations to SDK Harness
from apache_beam.options.pipeline_options import DebugOptions, PipelineOptions

import pyflink.fn_execution.beam.beam_operations # noqa # pylint: disable=unused-import

# force to register the coders to SDK Harness
import pyflink.fn_execution.beam.beam_coders # noqa # pylint: disable=unused-import

import apache_beam

# disable bundle processor shutdown
from apache_beam.runners.worker import sdk_worker, sdk_worker_main, statecache
sdk_worker.DEFAULT_BUNDLE_PROCESSOR_CACHE_SHUTDOWN_THRESHOLD_S = 86400 * 30


# Currently PyFlink only support count-based state cache strategy
def get_deep_size(*objs):
    return 1


def get_state_cache_size(options):
    """
    Return the maximum size of state cache in count.
    """
    if isinstance(options, PipelineOptions):
        experiments = options.view_as(DebugOptions).experiments or []
    else:
        experiments = options

    for experiment in experiments:
        # There should only be 1 match so returning from the loop
        if re.match(r'state_cache_size=', experiment):
            return int(
                re.match(r'state_cache_size=(?P<state_cache_size>.*)',
                         experiment).group('state_cache_size'))
    return 0


statecache.get_deep_size = get_deep_size
sdk_worker_main._get_state_cache_size = get_state_cache_size
# since Beam 2.52.0, _get_state_cache_size is renamed to _get_state_cache_size_bytes
sdk_worker_main._get_state_cache_size_bytes = get_state_cache_size


def print_to_logging(logging_func, msg, *args, **kwargs):
    if msg != '\n':
        logging_func(msg, *args, **kwargs)


class CustomPrint(object):
    def __init__(self, _print):
        self._msg_buffer = []
        self._print = _print

    def print(self, *args, sep=' ', end='\n', file=None):
        self._msg_buffer.append(sep.join([str(arg) for arg in args]))
        if end == '\n':
            self._print(''.join(self._msg_buffer), sep=sep, end=end, file=file)
            self._msg_buffer.clear()
        else:
            self._msg_buffer.append(end)

    def close(self):
        if self._msg_buffer:
            self._print(''.join(self._msg_buffer), sep='', end='\n')
            self._msg_buffer.clear()


def main():
    import builtins
    import logging
    from functools import partial

    # redirect stdout to logging.info, stderr to logging.error
    _info = logging.getLogger().info
    _error = logging.getLogger().error
    sys.stdout.write = partial(print_to_logging, _info)
    sys.stderr.write = partial(print_to_logging, _error)

    custom_print = CustomPrint(print)
    builtins.print = custom_print.print
    # Remove all the built-in log handles
    logging.getLogger().handlers = []
    apache_beam.runners.worker.sdk_worker_main.main(sys.argv)
    custom_print.close()
