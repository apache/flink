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

MAPPED_FILE_SIZE = 1024 * 1024 * 64

SIGNAL_REQUEST_BUFFER = b"\x00\x00\x00\x00"
SIGNAL_REQUEST_BUFFER_G0 = b"\xFF\xFF\xFF\xFD"
SIGNAL_REQUEST_BUFFER_G1 = b"\xFF\xFF\xFF\xFC"
SIGNAL_FINISHED = b"\xFF\xFF\xFF\xFF"

if PY2:
    SIGNAL_WAS_LAST = "\x20"
else:
    SIGNAL_WAS_LAST = 32


class OneWayBusyBufferingMappedFileConnection(object):
    def __init__(self, output_path):
        self._output_file = open(output_path, "rb+")
        self._file_output_buffer = mmap.mmap(self._output_file.fileno(), MAPPED_FILE_SIZE, mmap.MAP_SHARED, mmap.ACCESS_WRITE)

        self._out = deque()
        self._out_size = 0

        self._offset_limit = MAPPED_FILE_SIZE - 1024 * 1024 * 3

    def write(self, msg):
        self._out.append(msg)
        self._out_size += len(msg)
        if self._out_size > self._offset_limit:
            self._write_buffer()

    def _write_buffer(self):
        self._file_output_buffer.seek(1, 0)
        self._file_output_buffer.write(b"".join(self._out))
        self._file_output_buffer.seek(0, 0)
        self._file_output_buffer.write(b'\x01')


class BufferingTCPMappedFileConnection(object):
    def __init__(self, input_file, output_file, port):
        self._input_file = open(input_file, "rb+")
        self._output_file = open(output_file, "rb+")
        self._file_input_buffer = mmap.mmap(self._input_file.fileno(), MAPPED_FILE_SIZE, mmap.MAP_SHARED, mmap.ACCESS_READ)
        self._file_output_buffer = mmap.mmap(self._output_file.fileno(), MAPPED_FILE_SIZE, mmap.MAP_SHARED, mmap.ACCESS_WRITE)
        self._socket = SOCKET.socket(family=SOCKET.AF_INET, type=SOCKET.SOCK_STREAM)
        self._socket.connect((SOCKET.gethostbyname("localhost"), port))

        self._out = deque()
        self._out_size = 0

        self._input = b""
        self._input_offset = 0
        self._input_size = 0
        self._was_last = False

    def write(self, msg):
        length = len(msg)
        if length > MAPPED_FILE_SIZE:
            raise Exception("Serialized object does not fit into a single buffer.")
        tmp = self._out_size + length
        if tmp > MAPPED_FILE_SIZE:
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
        self._socket.recv(1, SOCKET.MSG_WAITALL)

    def read(self, des_size, ignored=None):
        if self._input_size == self._input_offset:
            self._read_buffer()
        old_offset = self._input_offset
        self._input_offset += des_size
        return self._input[old_offset:self._input_offset]

    def _read_buffer(self):
        self._socket.send(SIGNAL_REQUEST_BUFFER)
        self._file_input_buffer.seek(0, 0)
        self._input_offset = 0
        meta_size = self._socket.recv(5, SOCKET.MSG_WAITALL)
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


class TwinBufferingTCPMappedFileConnection(BufferingTCPMappedFileConnection):
    def __init__(self, input_file, output_file, port):
        super(TwinBufferingTCPMappedFileConnection, self).__init__(input_file, output_file, port)
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
        meta_size = self._socket.recv(5, SOCKET.MSG_WAITALL)
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


