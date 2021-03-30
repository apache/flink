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

from pyflink.serializers import IterableSerializer
from pyflink.table.utils import arrow_to_pandas, pandas_to_arrow


class ArrowSerializer(IterableSerializer):
    """
    Serializes pandas.Series into Arrow streaming format data.
    """

    def __init__(self, schema, row_type, timezone):
        super(ArrowSerializer, self).__init__()
        self._schema = schema
        self._field_types = row_type.field_types()
        self._timezone = timezone

    def __repr__(self):
        return "ArrowSerializer"

    def serialize(self, iterable, stream):
        writer = None
        try:
            for cols in iterable:
                batch = pandas_to_arrow(self._schema, self._timezone, self._field_types, cols)
                if writer is None:
                    import pyarrow as pa
                    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
                writer.write_batch(batch)
        finally:
            if writer is not None:
                writer.close()

    def deserialize(self, stream):
        import pyarrow as pa
        reader = pa.ipc.open_stream(stream)
        for batch in reader:
            yield arrow_to_pandas(self._timezone, self._field_types, [batch])

    def load_from_iterator(self, iter):
        class IteratorIO(io.RawIOBase):
            def __init__(self, iter):
                super(IteratorIO, self).__init__()
                self.iter = iter
                self.leftover = None

            def readable(self):
                return True

            def readinto(self, b):
                output_buffer_len = len(b)
                input = self.leftover or (self.iter.next() if self.iter.hasNext() else None)
                if input is None:
                    return 0
                output, self.leftover = input[:output_buffer_len], input[output_buffer_len:]
                b[:len(output)] = output
                return len(output)
        import pyarrow as pa
        reader = pa.ipc.open_stream(
            io.BufferedReader(IteratorIO(iter), buffer_size=io.DEFAULT_BUFFER_SIZE))
        for batch in reader:
            yield batch
