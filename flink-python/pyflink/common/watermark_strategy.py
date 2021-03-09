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
import abc

from typing import Any

from pyflink.common.time import Duration
from pyflink.java_gateway import get_gateway


class WatermarkStrategy(object):
    """
    The WatermarkStrategy defines how to generate Watermarks in the stream sources. The
    WatermarkStrategy is a builder/factory for the WatermarkGenerator that generates the watermarks
    and the TimestampAssigner which assigns the internal timestamp of a record.

    The convenience methods, for example forBoundedOutOfOrderness(Duration), create a
    WatermarkStrategy for common built in strategies.
    """
    def __init__(self, j_watermark_strategy):
        self._j_watermark_strategy = j_watermark_strategy
        self._timestamp_assigner = None

    def with_timestamp_assigner(self, timestamp_assigner: 'TimestampAssigner') -> \
            'WatermarkStrategy':
        """
        Creates a new WatermarkStrategy that wraps this strategy but instead uses the given a
        TimestampAssigner by implementing TimestampAssigner interface.

        ::
            >>> watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()
            >>>   .with_timestamp_assigner(MyTimestampAssigner())

        :param timestamp_assigner: The given TimestampAssigner.
        :return: A WaterMarkStrategy that wraps a TimestampAssigner.
        """
        self._timestamp_assigner = timestamp_assigner
        return self

    def with_idleness(self, idle_timeout: Duration) -> 'WatermarkStrategy':
        """
        Creates a new enriched WatermarkStrategy that also does idleness detection in the created
        WatermarkGenerator.

        :param idle_timeout: The idle timeout.
        :return: A new WatermarkStrategy with idle detection configured.
        """
        self._j_watermark_strategy = self._j_watermark_strategy\
            .withIdleness(idle_timeout._j_duration)
        return self

    @staticmethod
    def for_monotonous_timestamps():
        """
        Creates a watermark strategy for situations with monotonously ascending timestamps.

        The watermarks are generated periodically and tightly follow the latest timestamp in the
        data. The delay introduced by this strategy is mainly the periodic interval in which the
        watermarks are generated.
        """
        JWaterMarkStrategy = get_gateway().jvm\
            .org.apache.flink.api.common.eventtime.WatermarkStrategy
        return WatermarkStrategy(JWaterMarkStrategy.forMonotonousTimestamps())

    @staticmethod
    def for_bounded_out_of_orderness(max_out_of_orderness: Duration):
        """
        Creates a watermark strategy for situations where records are out of order, but you can
        place an upper bound on how far the events are out of order. An out-of-order bound B means
        that once the an event with timestamp T was encountered, no events older than (T - B) will
        follow any more.
        """
        JWaterMarkStrategy = get_gateway().jvm \
            .org.apache.flink.api.common.eventtime.WatermarkStrategy
        return WatermarkStrategy(
            JWaterMarkStrategy.forBoundedOutOfOrderness(max_out_of_orderness._j_duration))


class TimestampAssigner(abc.ABC):
    """
    A TimestampAssigner assigns event time timestamps to elements. These timestamps are used by all
    functions that operate on event time, for example event time windows.

    Timestamps can be an arbitrary int value, but all built-in implementations represent it as the
    milliseconds since the Epoch (midnight, January 1, 1970 UTC), the same way as time.time() does
    it.
    """
    @abc.abstractmethod
    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        """
        Assigns a timestamp to an element, in milliseconds since the Epoch. This is independent of
        any particular time zone or calendar.

        The method is passed the previously assigned timestamp of the element.
        That previous timestamp may have been assigned from a previous assigner. If the element did
        not carry a timestamp before, this value is the minimum value of int type.

        :param value: The element that the timestamp will be assigned to.
        :param record_timestamp: The current internal timestamp of the element, or a negative value,
                                 if no timestamp has been assigned yet.
        :return: The new timestamp.
        """
        pass
