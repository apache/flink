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
from avro.io import DatumReader, SchemaResolutionException, BinaryDecoder


class FlinkAvroDecoder(BinaryDecoder):

    def __init__(self, reader):
        super(FlinkAvroDecoder, self).__init__(reader)

    def read_int(self):
        return ((ord(self.read(1)) << 24) +
                (ord(self.read(1)) << 16) +
                (ord(self.read(1)) << 8) +
                ord(self.read(1)))

    def read_long(self):
        return ((ord(self.read(1)) << 56) +
                (ord(self.read(1)) << 48) +
                (ord(self.read(1)) << 40) +
                (ord(self.read(1)) << 32) +
                (ord(self.read(1)) << 24) +
                (ord(self.read(1)) << 16) +
                (ord(self.read(1)) << 8) +
                ord(self.read(1)))

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
