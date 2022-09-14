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
import struct


class InputStream(object):
    """
    A pure Python implementation of InputStream.
    """

    def __init__(self, data):
        self.data = data
        self.pos = 0

    def read(self, size):
        self.pos += size
        return self.data[self.pos - size: self.pos]

    def read_byte(self):
        self.pos += 1
        return self.data[self.pos - 1]

    def read_int8(self):
        return struct.unpack('b', self.read(1))[0]

    def read_int16(self):
        return struct.unpack('>h', self.read(2))[0]

    def read_int32(self):
        return struct.unpack('>i', self.read(4))[0]

    def read_int64(self):
        return struct.unpack('>q', self.read(8))[0]

    def read_float(self):
        return struct.unpack('>f', self.read(4))[0]

    def read_double(self):
        return struct.unpack('>d', self.read(8))[0]

    def read_bytes(self):
        size = self.read_int32()
        return self.read(size)

    def read_var_int64(self):
        shift = 0
        result = 0
        while True:
            byte = self.read_byte()
            if byte < 0:
                raise RuntimeError('VarLong not terminated.')

            bits = byte & 0x7F
            if shift >= 64 or (shift >= 63 and bits > 1):
                raise RuntimeError('VarLong too long.')
            result |= bits << shift
            shift += 7
            if not byte & 0x80:
                break
        if result >= 1 << 63:
            result -= 1 << 64
        return result

    def size(self):
        return len(self.data) - self.pos


class OutputStream(object):
    """
    A pure Python implementation of OutputStream.
    """

    def __init__(self):
        self.data = []
        self.byte_count = 0

    def write(self, b: bytes):
        self.data.append(b)
        self.byte_count += len(b)

    def write_byte(self, v):
        self.data.append(chr(v).encode('latin-1'))
        self.byte_count += 1

    def write_int8(self, v: int):
        self.write(struct.pack('b', v))

    def write_int16(self, v: int):
        self.write(struct.pack('>h', v))

    def write_int32(self, v: int):
        self.write(struct.pack('>i', v))

    def write_int64(self, v: int):
        self.write(struct.pack('>q', v))

    def write_float(self, v: float):
        self.write(struct.pack('>f', v))

    def write_double(self, v: float):
        self.write(struct.pack('>d', v))

    def write_bytes(self, v: bytes, size: int):
        self.write_int32(size)
        self.write(v[:size])

    def write_var_int64(self, v: int):
        if v < 0:
            v += 1 << 64
            if v <= 0:
                raise ValueError('Value too large (negative).')
        while True:
            bits = v & 0x7F
            v >>= 7
            if v:
                bits |= 0x80
            self.data.append(chr(bits).encode('latin-1'))
            self.byte_count += 1
            if not v:
                break

    def get(self) -> bytes:
        return b''.join(self.data)

    def size(self) -> int:
        return self.byte_count

    def clear(self):
        self.data.clear()
        self.byte_count = 0
