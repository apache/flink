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

import pickle
import struct
from abc import abstractmethod
from io import BytesIO
from itertools import chain
from typing import TypeVar, Iterable

from pyflink.common.serializer import TypeSerializer

T = TypeVar('T')


class SpecialLengths(object):
    END_OF_DATA_SECTION = -1
    NULL = -2


class IterableSerializer(TypeSerializer[Iterable[T]]):

    def _load_from_stream_without_unbatching(self, stream: BytesIO):
        """
        Returns an iterator of deserialized batches (iterable) of objects from the input stream.
        If the serializer does not operate on batches the default implementation returns an
        iterator of single element lists.
        """
        return map(lambda x: [x], self.deserialize(stream))


class VarLengthDataSerializer(IterableSerializer):
    """
    Serializer that writes objects as a stream of (length, data) pairs,
    where length is a 32-bit integer and data is length bytes.
    """

    def serialize(self, iterable, stream):
        for obj in iterable:
            self._write_with_length(obj, stream)

    def deserialize(self, stream):
        while True:
            try:
                yield self._read_with_length(stream)
            except EOFError:
                return

    def _write_with_length(self, obj, stream):
        serialized = self.dumps(obj)
        if serialized is None:
            raise ValueError("Serialized value should not be None")
        if len(serialized) > (1 << 31):
            raise ValueError("Can not serialize object larger than 2G")
        write_int(len(serialized), stream)
        stream.write(serialized)

    def _read_with_length(self, stream):
        length = read_int(stream)
        if length == SpecialLengths.END_OF_DATA_SECTION:
            raise EOFError
        elif length == SpecialLengths.NULL:
            return None
        obj = stream.read(length)
        if len(obj) < length:
            raise EOFError
        return self.loads(obj)

    @abstractmethod
    def dumps(self, obj):
        """
        Serialize an object into a byte array.
        When batching is used, this will be called with an array of objects.
        """
        pass

    @abstractmethod
    def loads(self, obj):
        """
        Deserialize an object from a byte array.
        """
        pass


class PickleSerializer(VarLengthDataSerializer):
    """
    Serializes objects using Python's pickle serializer:

        http://docs.python.org/3/library/pickle.html

    This serializer supports nearly any Python object, but may
    not be as fast as more specialized serializers.
    """

    def dumps(self, obj):
        return pickle.dumps(obj, 3)

    def loads(self, obj):
        return pickle.loads(obj, encoding="bytes")


class BatchedSerializer(IterableSerializer):
    """
    Serializes a stream of objects in batches by calling its wrapped
    Serializer with streams of objects.
    """

    UNLIMITED_BATCH_SIZE = -1
    UNKNOWN_BATCH_SIZE = 0

    def __init__(self, serializer, batch_size=UNLIMITED_BATCH_SIZE):
        self.serializer = serializer
        self.batch_size = batch_size

    def __repr__(self):
        return "BatchedSerializer(%s, %d)" % (str(self.serializer), self.batch_size)

    def _batched(self, iterable):
        if self.batch_size == self.UNLIMITED_BATCH_SIZE:
            yield list(iterable)
        elif hasattr(iterable, "__len__") and hasattr(iterable, "__getslice__"):
            n = len(iterable)
            for i in range(0, n, self.batch_size):
                yield iterable[i: i + self.batch_size]
        else:
            items = []
            count = 0
            for item in iterable:
                items.append(item)
                count += 1
                if count == self.batch_size:
                    yield items
                    items = []
                    count = 0
            if items:
                yield items

    def serialize(self, iterator, stream):
        self.serializer.serialize(self._batched(iterator), stream)

    def deserialize(self, stream):
        return chain.from_iterable(self._load_from_stream_without_unbatching(stream))

    def _load_from_stream_without_unbatching(self, stream):
        return self.serializer.deserialize(stream)


def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    return struct.unpack("!i", length)[0]


def write_int(value, stream):
    stream.write(struct.pack("!i", value))
