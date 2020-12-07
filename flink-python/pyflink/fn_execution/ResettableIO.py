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
import io


class ResettableIO(io.RawIOBase):
    """
    Raw I/O implementation the input and output stream is resettable.
    """

    def set_input_bytes(self, b):
        self._input_bytes = b
        self._input_offset = 0

    def readinto(self, b):
        """
        Read up to len(b) bytes into the writable buffer *b* and return
        the number of bytes read. If no bytes are available, None is returned.
        """
        input_len = len(self._input_bytes)
        output_buffer_len = len(b)
        remaining = input_len - self._input_offset

        if remaining >= output_buffer_len:
            b[:] = self._input_bytes[self._input_offset:self._input_offset + output_buffer_len]
            self._input_offset += output_buffer_len
            return output_buffer_len
        elif remaining > 0:
            b[:remaining] = self._input_bytes[self._input_offset:self._input_offset + remaining]
            self._input_offset = input_len
            return remaining
        else:
            return None

    def set_output_stream(self, output_stream):
        self._output_stream = output_stream

    def write(self, b):
        """
        Write the given bytes or pyarrow.Buffer object *b* to the underlying
        output stream and return the number of bytes written.
        """
        if isinstance(b, bytes):
            self._output_stream.write(b)
        else:
            # pyarrow.Buffer
            self._output_stream.write(b.to_pybytes())
        return len(b)

    def seekable(self):
        return False

    def readable(self):
        return True

    def writable(self):
        return True
