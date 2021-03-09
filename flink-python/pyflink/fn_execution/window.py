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
from abc import ABC, abstractmethod

MAX_LONG_VALUE = sys.maxsize


def long_to_int_with_bit_mixing(x: int) -> int:
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb
    x = x ^ (x >> 31)
    return x


def mod_inverse(x: int) -> int:
    inverse = x * x * x
    inverse *= 2 - x * inverse
    inverse *= 2 - x * inverse
    inverse *= 2 - x * inverse
    return inverse


class Window(ABC):
    """
    Window is a grouping of elements into finite buckets. Windows have a maximum timestamp
    which means that, at some point, all elements that go into one window will have arrived.
    """

    @abstractmethod
    def max_timestamp(self) -> int:
        pass


class TimeWindow(Window):
    """
    Window that represents a time interval from start (inclusive) to end (exclusive).
    """

    def __init__(self, start: int, end: int):
        super(TimeWindow, self).__init__()
        self.start = start
        self.end = end

    def max_timestamp(self) -> int:
        return self.end - 1

    def intersects(self, other: 'TimeWindow') -> bool:
        """
        Returns True if this window intersects the given window.
        """
        return self.start <= other.end and self.end >= other.start

    def cover(self, other: 'TimeWindow') -> 'TimeWindow':
        """
        Returns the minimal window covers both this window and the given window.
        """
        return TimeWindow(min(self.start, other.start), max(self.end, other.end))

    @staticmethod
    def get_window_start_with_offset(timestamp: int, offset: int, window_size: int):
        """
        Method to get the window start for a timestamp.

        :param timestamp: epoch millisecond to get the window start.
        :param offset: The offset which window start would be shifted by.
        :param window_size: The size of the generated windows.
        :return: window start
        """
        return timestamp - (timestamp - offset + window_size) % window_size

    def __hash__(self):
        return self.start + mod_inverse((self.end << 1) + 1)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.end == other.end \
            and self.start == other.start

    def __lt__(self, other: 'TimeWindow'):
        if not isinstance(other, TimeWindow):
            raise Exception("Does not support comparison with non-TimeWindow %s" % other)

        return self.start == other.start and self.end < other.end or self.start < other.start

    def __le__(self, other: 'TimeWindow'):
        return self.__eq__(other) and self.__lt__(other)

    def __repr__(self):
        return "TimeWindow(start={}, end={})".format(self.start, self.end)


class CountWindow(Window):
    """
    A Window that represents a count window. For each count window, we will assign a unique
    id. Thus this CountWindow can act as namespace part in state. We can attach data to each
    different CountWindow.
    """

    def __init__(self, id: int):
        super(CountWindow, self).__init__()
        self.id = id

    def max_timestamp(self) -> int:
        return MAX_LONG_VALUE

    def __hash__(self):
        return long_to_int_with_bit_mixing(self.id)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.id == other.id

    def __repr__(self):
        return "CountWindow(id={})".format(self.id)
