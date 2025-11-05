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
import collections
import threading
from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List

from pyflink.fn_execution.datastream.process.async_function import LONG_MIN_VALUE
from pyflink.fn_execution.datastream.process.input_handler import _emit_results

OUT = TypeVar('OUT')


class ResultFuture(Generic[OUT]):
    """
    Collects data / error in user codes while processing async i/o.
    """

    @abstractmethod
    def complete(self, result: List[OUT]):
        """
        Completes the result future with a collection of result objects.

        Note that it should be called for exactly one time in the user code. Calling this function
        for multiple times will cause data lose.

        Put all results in a collection and then emit output.

        :param result: A list of results.
        """
        pass

    @abstractmethod
    def complete_exceptionally(self, error: Exception):
        """
        Completes the result future exceptionally with an exception.

        :param error: An Exception object.
        """
        pass


class StreamElementQueueEntry(ABC, ResultFuture, Generic[OUT]):
    """
    An entry for the StreamElementQueue. The stream element queue entry stores the
    StreamElement for which the stream element queue entry has been instantiated. Furthermore, it
    allows to set the result of a completed entry through ResultFuture.
    """

    @abstractmethod
    def is_done(self) -> bool:
        """
        True if the stream element queue entry has been completed; otherwise false.
        """
        pass

    @abstractmethod
    def emit_result(self, output_processor) -> int:
        """
        Emits the results associated with this queue entry.

        :return: The number of popped input elements.
        """
        pass

    def complete_exceptionally(self, error: Exception):
        """
        Exceptions should be handled in the ResultHandler.
        """
        raise Exception("This result future should only be used to set completed results.")


class StreamRecordQueueEntry(StreamElementQueueEntry):
    """
    StreamElementQueueEntry implementation for StreamRecord. This class also acts as
    the ResultFuture implementation which is given to the AsyncFunction. The async
    function completes this class with a collection of results.
    """

    def __init__(self, windowed_value, timestamp, watermark, record):
        self._windowed_value = windowed_value
        self._record = record
        self._timestamp = timestamp
        self._watermark = watermark
        self._completed_results = None
        self._on_complete_handler = None

    def is_done(self) -> bool:
        return self._completed_results is not None

    def emit_result(self, output_processor):
        output_processor.process_outputs(
            self._windowed_value,
            _emit_results(self._timestamp, self._watermark, self._completed_results, False))
        return 1

    def on_complete(self, handler):
        self._on_complete_handler = handler

    def complete(self, result: List[OUT]):
        self._completed_results = result
        if self._on_complete_handler is not None:
            self._on_complete_handler(self)


class WatermarkQueueEntry(StreamElementQueueEntry):
    """
    StreamElementQueueEntry implementation for Watermark.
    """

    def __init__(self, watermark):
        self._watermark = watermark

    def is_done(self) -> bool:
        return True

    def emit_result(self, output_processor):
        # watermark will be passed together with the record
        return 0

    def complete(self, result: List[OUT]):
        raise Exception("Cannot complete a watermark.")


class StreamElementQueue(ABC, Generic[OUT]):

    @abstractmethod
    def put(self, windowed_value, timestamp, watermark, record) -> ResultFuture[OUT]:
        """
        Put the given record in the queue. This operation blocks until the queue has
        capacity left.

        This method returns a handle to the inserted element that allows to set the result of the
        computation.

        :param windowed_value: The windowed value for the record to be inserted.
        :param timestamp: The timestamp of the record to be inserted.
        :param watermark: The watermark of the record to be inserted.
        :param record: The actual record to be inserted.
        :return: A handle to the element.
        """
        pass

    @abstractmethod
    def advance_watermark(self, watermark):
        """
        Tries to put the given watermark in the queue. This operation succeeds if the queue has
        capacity left and fails if the queue is full.

        :param watermark: The watermark to be inserted.
        """
        pass

    @abstractmethod
    def emit_completed_element(self, output_processor):
        """
        Emits one completed element from the head of this queue into the given output.

        Will not emit any element if no element has been completed.
        """
        pass

    @abstractmethod
    def has_completed_elements(self) -> bool:
        """
        Checks if there is at least one completed head element.
        """
        pass

    @abstractmethod
    def wait_for_completed_elements(self):
        """
        Waits until there is completed elements.
        """
        pass

    @abstractmethod
    def wait_for_in_flight_elements_processed(self, timeout=1):
        """
        Waits until any inflight elements have been processed.
        """
        pass

    @abstractmethod
    def is_empty(self) -> bool:
        """
        True if the queue is empty; otherwise false.
        """
        pass

    @abstractmethod
    def size(self) -> int:
        """
        Return the size of the queue.
        """
        pass


class UnorderedStreamElementQueue(StreamElementQueue):

    class Segment(object):

        def __init__(self, capacity):
            self._incomplete_elements = set()
            self._completed_elements = collections.deque(maxlen=capacity)

        def add(self, entry: StreamElementQueueEntry):
            """
            Adds the given entry to this segment. If the element is completed (watermark), it is
            directly moved into the completed queue.
            """
            if entry.is_done():
                self._completed_elements.append(entry)
            else:
                self._incomplete_elements.add(entry)

        def completed(self, entry: StreamElementQueueEntry):
            """
            Signals that an entry finished computation.

            Adding only to completed queue if not completed before
            there may be a real result coming after a timeout result, which is updated in the
            queue entry but the entry is not re-added to the complete queue
            """
            try:
                self._incomplete_elements.remove(entry)
                self._completed_elements.append(entry)
            except KeyError:
                pass

        def emit_completed(self, output_processor) -> int:
            """
            Pops one completed elements into the given output. Because an input element may produce
            an arbitrary number of output elements, there is no correlation between the size of the
            collection and the popped elements.

            :return: The number of popped input elements.
            """
            if len(self._completed_elements) == 0:
                return 0

            completed_entry = self._completed_elements.popleft()
            return completed_entry.emit_result(output_processor)

        def is_empty(self):
            """
            True if there are no incomplete elements and all complete elements have been consumed.
            """
            return len(self._incomplete_elements) == 0 and len(self._completed_elements) == 0

        def has_completed(self):
            """
            True if there is at least one completed elements.
            """
            return len(self._completed_elements) > 0

    class SegmentedStreamRecordQueueEntry(StreamRecordQueueEntry):
        """
        An entry that notifies the respective segment upon completion.
        """

        def __init__(self, windowed_value, timestamp, watermark, record, segment):
            super().__init__(windowed_value, timestamp, watermark, record)
            self._segment = segment

        def get_segment(self):
            return self._segment

    def __init__(self, capacity: int, exception_checker):
        self._capacity = capacity
        self._exception_checker = exception_checker
        self._segments = collections.deque()
        self._lock = threading.RLock()
        self._not_full = threading.Condition(self._lock)
        self._not_empty = threading.Condition(self._lock)
        self._number_of_pending_entries = 0
        self._current_watermark = LONG_MIN_VALUE

    def put(self, windowed_value, timestamp, watermark, record) -> ResultFuture[OUT]:
        with self._not_full:
            while self.size() >= self._capacity:
                self._not_full.wait(1)
                self._exception_checker()

            self._number_of_pending_entries += 1
            entry = self._add_record(windowed_value, timestamp, watermark, record)
            entry.on_complete(self.on_complete_handler)
            return entry

    def advance_watermark(self, watermark):
        if watermark > self._current_watermark:
            self._current_watermark = watermark

            with self._lock:
                self._add_watermark(watermark)

    def emit_completed_element(self, output_processor):
        with self._not_full:
            if len(self._segments) == 0:
                return

            current_segment = self._segments[0]
            self._number_of_pending_entries -= current_segment.emit_completed(output_processor)

            # remove any segment if there are further segments, if not leave it as an optimization
            # even if empty
            if len(self._segments) > 1 and current_segment.is_empty():
                self._segments.popleft()

            if self._number_of_pending_entries < self._capacity:
                self._not_full.notify_all()

    def has_completed_elements(self) -> bool:
        with self._lock:
            return len(self._segments) != 0 and self._segments[0].has_completed()

    def wait_for_completed_elements(self):
        with self._not_empty:
            while not self.has_completed_elements():
                self._not_empty.wait()

    def wait_for_in_flight_elements_processed(self, timeout=1):
        with self._not_full:
            if self._number_of_pending_entries != 0:
                self._not_full.wait(timeout)

    def is_empty(self) -> bool:
        with self._lock:
            return self._number_of_pending_entries == 0

    def size(self) -> int:
        with self._lock:
            return self._number_of_pending_entries

    def on_complete_handler(self, entry):
        with self._not_empty:
            entry.get_segment().completed(entry)
            if self.has_completed_elements():
                self._not_empty.notify()

    def _add_record(self, windowed_value, timestamp, watermark, record) -> \
            'UnorderedStreamElementQueue.SegmentedStreamRecordQueueEntry':
        if len(self._segments) == 0:
            last_segment = self._add_segment(self._capacity)
        else:
            last_segment = self._segments[-1]

        entry = UnorderedStreamElementQueue.SegmentedStreamRecordQueueEntry(
            windowed_value, timestamp, watermark, record, last_segment)
        last_segment.add(entry)
        return entry

    def _add_watermark(self, watermark):
        if len(self._segments) != 0 and self._segments[-1].is_empty():
            # reuse already existing segment if possible (completely drained) or the new segment
            # added at the end of this method for two succeeding watermarks
            watermark_segment = self._segments[-1]
        else:
            watermark_segment = self._add_segment(1)

        entry = WatermarkQueueEntry(watermark)
        watermark_segment.add(entry)

        self._add_segment(self._capacity)

    def _add_segment(self, capacity) -> 'UnorderedStreamElementQueue.Segment':
        new_segment = UnorderedStreamElementQueue.Segment(capacity)
        self._segments.append(new_segment)
        return new_segment


class OrderedStreamElementQueue(StreamElementQueue):

    def __init__(self, capacity: int, exception_checker):
        self._capacity = capacity
        self._exception_checker = exception_checker
        self._queue = collections.deque()
        self._lock = threading.RLock()
        self._not_full = threading.Condition(self._lock)
        self._not_empty = threading.Condition(self._lock)
        self._number_of_pending_entries = 0

    def put(self, windowed_value, timestamp, watermark, record) -> ResultFuture[OUT]:
        with self._not_full:
            while self.size() >= self._capacity:
                self._not_full.wait(1)
                self._exception_checker()

            entry = StreamRecordQueueEntry(windowed_value, timestamp, watermark, record)
            entry.on_complete(self.on_complete_handler)
            self._queue.append(entry)
            self._number_of_pending_entries += 1
            return entry

    def advance_watermark(self, watermark):
        # do nothing in ordered mode
        pass

    def emit_completed_element(self, output_processor):
        with self._not_full:
            if not self.has_completed_elements():
                return

            self._queue.popleft().emit_result(output_processor)
            self._number_of_pending_entries -= 1
            self._not_full.notify_all()

    def has_completed_elements(self) -> bool:
        with self._lock:
            return len(self._queue) > 0 and self._queue[0].is_done()

    def wait_for_completed_elements(self):
        with self._not_empty:
            while not self.has_completed_elements():
                self._not_empty.wait()

    def wait_for_in_flight_elements_processed(self, timeout=1):
        with self._not_full:
            if self._number_of_pending_entries != 0:
                self._not_full.wait(timeout)

    def is_empty(self) -> bool:
        with self._lock:
            return self._number_of_pending_entries == 0

    def size(self) -> int:
        with self._lock:
            return self._number_of_pending_entries

    def on_complete_handler(self, entry):
        with self._not_empty:
            if self.has_completed_elements():
                self._not_empty.notify()
