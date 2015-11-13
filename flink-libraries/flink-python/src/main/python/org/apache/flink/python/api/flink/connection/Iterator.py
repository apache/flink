# ###############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
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
from struct import unpack
from collections import deque

try:
    import _abcoll as defIter
except:
    import _collections_abc as defIter

from flink.connection.Constants import Types


class ListIterator(defIter.Iterator):
    def __init__(self, values):
        super(ListIterator, self).__init__()
        self._values = deque(values)

    def __next__(self):
        return self.next()

    def next(self):
        if self.has_next():
            return self._values.popleft()
        else:
            raise StopIteration

    def has_next(self):
        return self._values


class GroupIterator(defIter.Iterator):
    def __init__(self, iterator, keys=None):
        super(GroupIterator, self).__init__()
        self.iterator = iterator
        self.key = None
        self.keys = keys
        if self.keys is None:
            self._extract_keys = self._extract_keys_id
        self.cur = None
        self.empty = False

    def _init(self):
        if self.iterator.has_next():
            self.empty = False
            self.cur = self.iterator.next()
            self.key = self._extract_keys(self.cur)
        else:
            self.empty = True

    def __next__(self):
        return self.next()

    def next(self):
        if self.has_next():
            tmp = self.cur
            if self.iterator.has_next():
                self.cur = self.iterator.next()
                if self.key != self._extract_keys(self.cur):
                    self.empty = True
            else:
                self.cur = None
                self.empty = True
            return tmp
        else:
            raise StopIteration

    def has_next(self):
        if self.empty:
            return False
        return self.key == self._extract_keys(self.cur)

    def has_group(self):
        return self.cur is not None

    def next_group(self):
        self.key = self._extract_keys(self.cur)
        self.empty = False

    def _extract_keys(self, x):
        return [x[k] for k in self.keys]

    def _extract_keys_id(self, x):
        return x


class CoGroupIterator(object):
    NONE_REMAINED = 1
    FIRST_REMAINED = 2
    SECOND_REMAINED = 3
    FIRST_EMPTY = 4
    SECOND_EMPTY = 5

    def __init__(self, c1, c2, k1, k2):
        self.i1 = GroupIterator(c1, k1)
        self.i2 = GroupIterator(c2, k2)
        self.p1 = None
        self.p2 = None
        self.match = None
        self.key = None

    def _init(self):
        self.i1._init()
        self.i2._init()

    def next(self):
        first_empty = True
        second_empty = True

        if self.match != CoGroupIterator.FIRST_EMPTY:
            if self.match == CoGroupIterator.FIRST_REMAINED:
                first_empty = False
            else:
                if self.i1.has_group():
                    self.i1.next_group()
                    self.key = self.i1.key
                    first_empty = False

        if self.match != CoGroupIterator.SECOND_EMPTY:
            if self.match == CoGroupIterator.SECOND_REMAINED:
                second_empty = False
            else:
                if self.i2.has_group():
                    self.i2.next_group()
                    second_empty = False

        if first_empty and second_empty:
            return False
        elif first_empty and (not second_empty):
            self.p1 = DummyIterator()
            self.p2 = self.i2
            self.match = CoGroupIterator.FIRST_EMPTY
            return True
        elif (not first_empty) and second_empty:
            self.p1 = self.i1
            self.p2 = DummyIterator()
            self.match = CoGroupIterator.SECOND_EMPTY
            return True
        else:
            if self.key == self.i2.key:
                self.p1 = self.i1
                self.p2 = self.i2
                self.match = CoGroupIterator.NONE_REMAINED
            elif self.key < self.i2.key:
                self.p1 = self.i1
                self.p2 = DummyIterator()
                self.match = CoGroupIterator.SECOND_REMAINED
            else:
                self.p1 = DummyIterator()
                self.p2 = self.i2
                self.match = CoGroupIterator.FIRST_REMAINED
            return True


class Iterator(defIter.Iterator):
    def __init__(self, con, env, group=0):
        super(Iterator, self).__init__()
        self._connection = con
        self._init = True
        self._group = group
        self._deserializer = None
        self._env = env

    def __next__(self):
        return self.next()

    def _read(self, des_size):
        return self._connection.read(des_size, self._group)

    def next(self):
        if self.has_next():
            if self._deserializer is None:
                self._deserializer = _get_deserializer(self._group, self._connection.read, self._env._types)
            return self._deserializer.deserialize(self._read)
        else:
            raise StopIteration

    def has_next(self):
        return self._connection.has_next(self._group)

    def _reset(self):
        self._deserializer = None


class DummyIterator(Iterator):
    def __init__(self):
        super(Iterator, self).__init__()

    def __next__(self):
        raise StopIteration

    def next(self):
        raise StopIteration

    def has_next(self):
        return False


def _get_deserializer(group, read, custom_types, type=None):
    if type is None:
        type = read(1, group)
        return _get_deserializer(group, read, custom_types, type)
    elif type == Types.TYPE_TUPLE:
        return TupleDeserializer(read, group, custom_types)
    elif type == Types.TYPE_BYTE:
        return ByteDeserializer()
    elif type == Types.TYPE_BYTES:
        return ByteArrayDeserializer()
    elif type == Types.TYPE_BOOLEAN:
        return BooleanDeserializer()
    elif type == Types.TYPE_FLOAT:
        return FloatDeserializer()
    elif type == Types.TYPE_DOUBLE:
        return DoubleDeserializer()
    elif type == Types.TYPE_INTEGER:
        return IntegerDeserializer()
    elif type == Types.TYPE_LONG:
        return LongDeserializer()
    elif type == Types.TYPE_STRING:
        return StringDeserializer()
    elif type == Types.TYPE_NULL:
        return NullDeserializer()
    else:
        for entry in custom_types:
            if type == entry[0]:
                return entry[3]
        raise Exception("Unable to find deserializer for type ID " + str(type))


class TupleDeserializer(object):
    def __init__(self, read, group, custom_types):
        size = unpack(">I", read(4, group))[0]
        self.deserializer = [_get_deserializer(group, read, custom_types) for _ in range(size)]

    def deserialize(self, read):
        return tuple([s.deserialize(read) for s in self.deserializer])


class ByteDeserializer(object):
    def deserialize(self, read):
        return unpack(">c", read(1))[0]


class ByteArrayDeserializer(object):
    def deserialize(self, read):
        size = unpack(">i", read(4))[0]
        return bytearray(read(size)) if size else bytearray(b"")


class BooleanDeserializer(object):
    def deserialize(self, read):
        return unpack(">?", read(1))[0]


class FloatDeserializer(object):
    def deserialize(self, read):
        return unpack(">f", read(4))[0]


class DoubleDeserializer(object):
    def deserialize(self, read):
        return unpack(">d", read(8))[0]


class IntegerDeserializer(object):
    def deserialize(self, read):
        return unpack(">i", read(4))[0]


class LongDeserializer(object):
    def deserialize(self, read):
        return unpack(">q", read(8))[0]


class StringDeserializer(object):
    def deserialize(self, read):
        length = unpack(">i", read(4))[0]
        return read(length).decode("utf-8") if length else ""


class NullDeserializer(object):
    def deserialize(self):
        return None
