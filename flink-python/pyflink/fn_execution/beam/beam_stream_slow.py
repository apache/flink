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
from apache_beam.coders.coder_impl import create_InputStream, create_OutputStream

from pyflink.fn_execution.stream_slow import InputStream
from pyflink.fn_execution.utils.operation_utils import PeriodicThread


class BeamInputStream(InputStream):
    def __init__(self, input_stream: create_InputStream):
        super(BeamInputStream, self).__init__([])
        self._input_stream = input_stream

    def read(self, size):
        return self._input_stream.read(size)

    def read_byte(self):
        return self._input_stream.read_byte()

    def size(self):
        return self._input_stream.size()


class BeamTimeBasedOutputStream(create_OutputStream):
    def __init__(self):
        super(BeamTimeBasedOutputStream).__init__()
        self._flush_event = False
        self._periodic_flusher = PeriodicThread(1, self.notify_flush)
        self._periodic_flusher.daemon = True
        self._periodic_flusher.start()
        self._output_stream = None

    def write(self, b: bytes):
        self._output_stream.write(b)

    def reset_output_stream(self, output_stream: create_OutputStream):
        self._output_stream = output_stream

    def notify_flush(self):
        self._flush_event = True

    def close(self):
        if self._periodic_flusher:
            self._periodic_flusher.cancel()
            self._periodic_flusher = None

    def maybe_flush(self):
        if self._flush_event:
            self._output_stream.flush()
            self._flush_event = False
        else:
            self._output_stream.maybe_flush()
