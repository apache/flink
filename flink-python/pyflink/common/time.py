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
from pyflink.java_gateway import get_gateway

__all__ = ['Duration', 'Instant', 'Time']


class Duration(object):
    """
    A time-based amount of time, such as '34.5 seconds'.
    """
    def __init__(self, j_duration):
        self._j_duration = j_duration

    @staticmethod
    def of_days(days: int):
        return Duration(get_gateway().jvm.java.time.Duration.ofDays(days))

    @staticmethod
    def of_hours(hours: int):
        return Duration(get_gateway().jvm.java.time.Duration.ofHours(hours))

    @staticmethod
    def of_millis(millis: int):
        return Duration(get_gateway().jvm.java.time.Duration.ofMillis(millis))

    @staticmethod
    def of_minutes(minutes: int):
        return Duration(get_gateway().jvm.java.time.Duration.ofMinutes(minutes))

    @staticmethod
    def of_nanos(nanos: int):
        return Duration(get_gateway().jvm.java.time.Duration.ofNanos(nanos))

    @staticmethod
    def of_seconds(seconds: int):
        return Duration(get_gateway().jvm.java.time.Duration.ofSeconds(seconds))

    def __eq__(self, other):
        return isinstance(other, Duration) and self._j_duration.equals(other._j_duration)


class Instant(object):
    """
    An instantaneous point on the time-line. Similar to Java.time.Instant.
    """

    def __init__(self, seconds, nanos):
        self.seconds = seconds
        self.nanos = nanos

    def to_epoch_milli(self):
        if self.seconds < 0 < self.nanos:
            return (self.seconds + 1) * 1000 + self.nanos // 1000_1000 - 1000
        else:
            return self.seconds * 1000 + self.nanos // 1000_000

    @staticmethod
    def of_epoch_milli(epoch_milli: int) -> 'Instant':
        secs = epoch_milli // 1000
        mos = epoch_milli % 1000
        return Instant(secs, mos * 1000_000)

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.seconds == other.seconds and
                self.nanos == other.nanos)

    def __repr__(self):
        return 'Instant<{}, {}>'.format(self.seconds, self.nanos)


class Time(object):
    """
    The definition of a time interval.
    """

    def __init__(self, milliseconds: int):
        self._milliseconds = milliseconds

    def to_milliseconds(self) -> int:
        return self._milliseconds

    @staticmethod
    def milliseconds(milliseconds: int):
        return Time(milliseconds)

    @staticmethod
    def seconds(seconds: int):
        return Time.milliseconds(seconds * 1000)

    @staticmethod
    def minutes(minutes: int):
        return Time.seconds(minutes * 60)

    @staticmethod
    def hours(hours: int):
        return Time.minutes(hours * 60)

    @staticmethod
    def days(days: int):
        return Time.hours(days * 24)

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self._milliseconds == other._milliseconds)

    def __str__(self):
        return "{} ms".format(self._milliseconds)
