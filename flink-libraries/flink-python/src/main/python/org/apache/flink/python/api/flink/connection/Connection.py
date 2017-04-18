# ###############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
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
import mmap
import socket as SOCKET
from struct import pack, unpack
from collections import deque
import sys
PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

SIGNAL_REQUEST_BUFFER = b"\x00\x00\x00\x00"
SIGNAL_REQUEST_BUFFER_G0 = b"\xFF\xFF\xFF\xFD"
SIGNAL_REQUEST_BUFFER_G1 = b"\xFF\xFF\xFF\xFC"
SIGNAL_FINISHED = b"\xFF\xFF\xFF\xFF"

if PY2:
    SIGNAL_WAS_LAST = "\x20"
else:
    SIGNAL_WAS_LAST = 32


def recv_all(socket, toread):
    initial = socket.recv(toread)
    bytes_read = len(initial)
    if bytes_read == toread:
        return initial
    else:
        bits = [initial]
        toread = toread - bytes_read
        while toread:
            bit = socket.recv(toread)
            bits.append(bit)
            toread = toread - len(bit)
        return b"".join(bits)


class PureTCPConnection(object):
    def __init__(self, port):
        self._socket = SOCKET.socket(family=SOCKET.AF_INET, type=SOCKET.SOCK_STREAM)
        self._socket.connect((SOCKET.gethostbyname("localhost"), port))

    def write(self, msg):
        self._socket.send(msg)

    def read(self, size):
        return recv_all(self._socket, size)

    def close(self):
        self._socket.close()


class BufferingTCPMappedFileConnection(object):
    def __init__(self, input_file, output_file, mmap_size, port):
        self._input_file = open(input_file, "rb+")
        self._output_file = open(output_file, "rb+")
        self._mmap_size = mmap_size
        if hasattr(mmap, 'MAP_SHARED'):
            self._file_input_buffer = mmap.mmap(self._input_file.fileno(), mmap_size, mmap.MAP_SHARED, mmap.ACCESS_READ)
            self._file_output_buffer = mmap.mmap(self._output_file.fileno(), mmap_size, mmap.MAP_SHARED, mmap.ACCESS_WRITE)
        else:
            self._file_input_buffer = mmap.mmap(self._input_file.fileno(), mmap_size, None, mmap.ACCESS_READ)
            self._file_output_buffer = mmap.mmap(self._output_file.fileno(), mmap_size, None, mmap.ACCESS_WRITE)
        self._socket = SOCKET.socket(family=SOCKET.AF_INET, type=SOCKET.SOCK_STREAM)
        self._socket.connect((SOCKET.gethostbyname("localhost"), port))

        self._out = deque()
        self._out_size = 0

        self._input = b""
        self._input_offset = 0
        self._input_size = 0
        self._was_last = False

    def close(self):
        self._socket.close()

    def write(self, msg):
        length = len(msg)
        if length > self._mmap_size:
            raise Exception("Serialized object does not fit into a single buffer.")
        tmp = self._out_size + length
        if tmp > self._mmap_size:
            self._write_buffer()
            self.write(msg)
        else:
            self._out.append(msg)
            self._out_size = tmp

    def _write_buffer(self):
        self._file_output_buffer.seek(0, 0)
        self._file_output_buffer.write(b"".join(self._out))
        self._socket.send(pack(">i", self._out_size))
        self._out.clear()
        self._out_size = 0
        recv_all(self._socket, 1)

    def read(self, des_size, ignored=None):
        if self._input_size == self._input_offset:
            self._read_buffer()
        old_offset = self._input_offset
        self._input_offset += des_size
        if self._input_offset > self._input_size:
            raise Exception("BufferUnderFlowException")
        return self._input[old_offset:self._input_offset]

    def _read_buffer(self):
        self._socket.send(SIGNAL_REQUEST_BUFFER)
        self._file_input_buffer.seek(0, 0)
        self._input_offset = 0
        meta_size = recv_all(self._socket, 5)
        self._input_size = unpack(">I", meta_size[:4])[0]
        self._was_last = meta_size[4] == SIGNAL_WAS_LAST
        self._input = self._file_input_buffer.read(self._input_size)

    def send_end_signal(self):
        if self._out_size:
            self._write_buffer()
        self._socket.send(SIGNAL_FINISHED)

    def has_next(self, ignored=None):
        return not self._was_last or not self._input_size == self._input_offset

    def reset(self):
        self._was_last = False
        self._input_size = 0
        self._input_offset = 0
        self._input = b""

    def read_secondary(self, des_size):
        return recv_all(self._socket, des_size)

    def write_secondary(self, data):
        self._socket.send(data)


class TwinBufferingTCPMappedFileConnection(BufferingTCPMappedFileConnection):
    def __init__(self, input_file, output_file, mmap_size, port):
        super(TwinBufferingTCPMappedFileConnection, self).__init__(input_file, output_file, mmap_size, port)
        self._input = [b"", b""]
        self._input_offset = [0, 0]
        self._input_size = [0, 0]
        self._was_last = [False, False]

    def read(self, des_size, group):
        if self._input_size[group] == self._input_offset[group]:
            self._read_buffer(group)
        old_offset = self._input_offset[group]
        self._input_offset[group] += des_size
        return self._input[group][old_offset:self._input_offset[group]]

    def _read_buffer(self, group):
        if group:
            self._socket.send(SIGNAL_REQUEST_BUFFER_G1)
        else:
            self._socket.send(SIGNAL_REQUEST_BUFFER_G0)
        self._file_input_buffer.seek(0, 0)
        self._input_offset[group] = 0
        meta_size = recv_all(self._socket, 5)
        self._input_size[group] = unpack(">I", meta_size[:4])[0]
        self._was_last[group] = meta_size[4] == SIGNAL_WAS_LAST
        self._input[group] = self._file_input_buffer.read(self._input_size[group])

    def has_next(self, group):
        return not self._was_last[group] or not self._input_size[group] == self._input_offset[group]

    def reset(self):
        self._was_last = [False, False]
        self._input_size = [0, 0]
        self._input_offset = [0, 0]
        self._input = [b"", b""]


