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
import sys

from apache_beam.coders.coder_impl import StreamCoderImpl

if sys.version > '3':
    xrange = range


class RowCoderImpl(StreamCoderImpl):

    def __init__(self, field_coders):
        self._field_coders = field_coders

    def encode_to_stream(self, value, out_stream, nested):
        self.write_null_mask(value, out_stream)
        for i in xrange(len(self._field_coders)):
            self._field_coders[i].encode_to_stream(value[i], out_stream, nested)

    def decode_from_stream(self, in_stream, nested):
        from pyflink.table import Row
        null_mask = self.read_null_mask(len(self._field_coders), in_stream)
        assert len(null_mask) == len(self._field_coders)
        return Row(*[None if null_mask[idx] else self._field_coders[idx].decode_from_stream(
            in_stream, nested) for idx in xrange(0, len(null_mask))])

    @staticmethod
    def write_null_mask(value, out_stream):
        field_pos = 0
        field_count = len(value)
        while field_pos < field_count:
            b = 0x00
            # set bits in byte
            num_pos = min(8, field_count - field_pos)
            byte_pos = 0
            while byte_pos < num_pos:
                b = b << 1
                # set bit if field is null
                if value[field_pos + byte_pos] is None:
                    b |= 0x01
                byte_pos += 1
            field_pos += num_pos
            # shift bits if last byte is not completely filled
            b <<= (8 - byte_pos)
            # write byte
            out_stream.write_byte(b)

    @staticmethod
    def read_null_mask(field_count, in_stream):
        null_mask = []
        field_pos = 0
        while field_pos < field_count:
            b = in_stream.read_byte()
            num_pos = min(8, field_count - field_pos)
            byte_pos = 0
            while byte_pos < num_pos:
                null_mask.append((b & 0x80) > 0)
                b = b << 1
                byte_pos += 1
            field_pos += num_pos
        return null_mask

    def __repr__(self):
        return 'RowCoderImpl[%s]' % ', '.join(str(c) for c in self._field_coders)
