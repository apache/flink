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

from avro.io import (
    AvroTypeException,
    BinaryDecoder,
    BinaryEncoder,
    DatumReader,
    DatumWriter,
    SchemaResolutionException,
    Validate,
)

STRUCT_FLOAT = struct.Struct('>f')  # big-endian float
STRUCT_DOUBLE = struct.Struct('>d')  # big-endian double
STRUCT_INT = struct.Struct('>i')  # big-endian int
STRUCT_LONG_LONG = struct.Struct('>q')  # big-endian long long


class FlinkAvroBufferWrapper(object):

    def __init__(self):
        self._stream = None

    def switch_stream(self, stream):
        self._stream = stream

    def read(self, n=1):
        return self._stream.read(n)

    def write(self, data):
        return self._stream.write(data)


class FlinkAvroDecoder(BinaryDecoder):
    """
    Flink-customized basic type decoder, with some different encodings, e.g. Default avro encode int
    and long into variable-sized bytes, while Flink use fixed bytes in some places.
    """

    def __init__(self, reader):
        super().__init__(reader)

    def read_int(self):
        return STRUCT_INT.unpack(self.read(4))[0]

    def read_long(self):
        return STRUCT_LONG_LONG.unpack(self.read(8))[0]

    def read_var_long(self):
        """
        Flink implementation of variable-sized long serialization does not move sign to lower bit
        and flip the rest.
        """
        b = ord(self.read(1))
        n = b & 0x7F
        shift = 7
        while (b & 0x80) != 0:
            b = ord(self.read(1))
            n |= (b & 0x7F) << shift
            shift += 7
        return n

    def read_float(self):
        return STRUCT_FLOAT.unpack(self.read(4))[0]

    def read_double(self):
        return STRUCT_DOUBLE.unpack(self.read(8))[0]

    def read_bytes(self):
        nbytes = self.read_int()
        assert (nbytes >= 0), nbytes
        return self.read(nbytes)

    def skip_int(self):
        self.skip(4)

    def skip_long(self):
        self.skip(8)

    def skip_bytes(self):
        nbytes = self.read_int()
        assert (nbytes >= 0), nbytes
        self.skip(nbytes)


class FlinkAvroDatumReader(DatumReader):
    """
    Flink-customized datum reader to parse composite data structure, to fit with changed basic type
    serialization in FlinkAvroDecoder.
    """

    def __init__(self, writer_schema=None, reader_schema=None):
        super().__init__(writer_schema, reader_schema)

    def read_array(self, writer_schema, reader_schema, decoder: 'FlinkAvroDecoder'):
        read_items = []
        block_count = decoder.read_var_long()
        assert block_count >= 0
        if block_count == 0:
            return read_items
        for i in range(block_count):
            read_items.append(self.read_data(writer_schema.items,
                                             reader_schema.items, decoder))
        decoder.read_var_long()
        return read_items

    def skip_array(self, writer_schema, decoder: 'FlinkAvroDecoder'):
        block_count = decoder.read_var_long()
        assert block_count >= 0
        if block_count == 0:
            return
        for i in range(block_count):
            self.skip_data(writer_schema.items, decoder)
        decoder.read_var_long()

    def read_map(self, writer_schema, reader_schema, decoder: 'FlinkAvroDecoder'):
        read_items = {}
        block_count = decoder.read_var_long()
        assert block_count >= 0
        if block_count == 0:
            return read_items
        for i in range(block_count):
            key = decoder.read_utf8()
            read_items[key] = self.read_data(writer_schema.values,
                                             reader_schema.values, decoder)
        decoder.read_var_long()
        return read_items

    def skip_map(self, writer_schema, decoder: 'FlinkAvroDecoder'):
        block_count = decoder.read_var_long()
        assert block_count >= 0
        if block_count == 0:
            return
        for i in range(block_count):
            decoder.skip_utf8()
            self.skip_data(writer_schema.values, decoder)
        decoder.read_long()

    def read_union(self, writer_schema, reader_schema, decoder: 'FlinkAvroDecoder'):
        index_of_schema = int(decoder.read_int())
        if index_of_schema >= len(writer_schema.schemas):
            fail_msg = "Can't access branch index %d for union with %d branches" \
                       % (index_of_schema, len(writer_schema.schemas))
            raise SchemaResolutionException(fail_msg, writer_schema, reader_schema)
        selected_writer_schema = writer_schema.schemas[index_of_schema]

        return self.read_data(selected_writer_schema, reader_schema, decoder)

    def skip_union(self, writer_schema, decoder):
        index_of_schema = int(decoder.read_int())
        if index_of_schema >= len(writer_schema.schemas):
            fail_msg = "Can't access branch index %d for union with %d branches" \
                       % (index_of_schema, len(writer_schema.schemas))
            raise SchemaResolutionException(fail_msg, writer_schema)
        return self.skip_data(writer_schema.schemas[index_of_schema], decoder)


class FlinkAvroEncoder(BinaryEncoder):

    def __init__(self, writer):
        super().__init__(writer)

    def write_int(self, datum):
        self.write(STRUCT_INT.pack(datum))

    def write_long(self, datum):
        self.write(STRUCT_LONG_LONG.pack(datum))

    def write_var_long(self, datum):
        while datum & 0x80 != 0:
            self.write((datum & 0x80).to_bytes(1, 'big'))
            datum <<= 7
        self.write(datum.to_bytes(1, 'big'))

    def write_float(self, datum):
        self.write(STRUCT_FLOAT.pack(datum))

    def write_double(self, datum):
        self.write(STRUCT_DOUBLE.pack(datum))

    def write_bytes(self, datum):
        self.write_int(len(datum))
        self.write(datum)


class FlinkAvroDatumWriter(DatumWriter):

    def __init__(self, writer_schema=None):
        super().__init__(writer_schema=writer_schema)

    def write_array(self, writer_schema, datum, encoder):
        if len(datum) > 0:
            encoder.write_var_long(len(datum))
            for item in datum:
                self.write_data(writer_schema.items, item, encoder)
        encoder.write_long(0)

    def write_map(self, writer_schema, datum, encoder):
        if len(datum) > 0:
            encoder.write_var_long(len(datum))
            for key, val in datum.items():
                encoder.write_utf8(key)
                self.write_data(writer_schema.values, val, encoder)
        encoder.write_var_long(0)

    def write_union(self, writer_schema, datum, encoder):
        # resolve union
        index_of_schema = -1
        for i, candidate_schema in enumerate(writer_schema.schemas):
            if Validate(candidate_schema, datum):
                index_of_schema = i
        if index_of_schema < 0:
            raise AvroTypeException(writer_schema, datum)

        # write data
        encoder.write_int(index_of_schema)
        self.write_data(writer_schema.schemas[index_of_schema], datum, encoder)
