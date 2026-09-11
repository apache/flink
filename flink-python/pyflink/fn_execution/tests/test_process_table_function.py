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
import datetime
import unittest
from types import SimpleNamespace
from unittest import mock

from pyflink.common import Instant, Row
from pyflink.fn_execution.table.process_table_function import (
    CLEAR_ALL,
    DELETE_ANONYMOUS,
    DELETE_NAMED,
    REGISTER_ANONYMOUS,
    REGISTER_NAMED,
    TRIGGER,
    ProcessTableFunctionOperation,
    _ProcessTableFunctionContext,
)


class _StateHandle(object):

    def __init__(self, value=None):
        self._value = value
        self.updated = []
        self.clear_count = 0

    def value(self):
        return self._value

    def update(self, value):
        self.updated.append(value)
        self._value = value

    def clear(self):
        self.clear_count += 1
        self._value = None


class _TimerEmitter(object):

    def __init__(self):
        self.commands = []

    def emit(self, operation, timestamp=None, name=None):
        self.commands.append((operation, timestamp, name))


class _KeyedBackend(object):

    def __init__(self):
        self.current_key = None

    def set_current_key(self, key):
        self.current_key = key


class _StateBackend(_KeyedBackend):

    def __init__(self):
        super(_StateBackend, self).__init__()
        self.requests = []

    def get_value_state(self, name, coder, ttl_config):
        handle = _StateHandle()
        self.requests.append((name, coder, ttl_config, handle))
        return handle


def _state_spec(name, *field_names):
    fields = [SimpleNamespace(name=field_name) for field_name in field_names]
    return SimpleNamespace(
        name=name,
        type=SimpleNamespace(row_schema=SimpleNamespace(fields=fields)))


def _state_spec_with_ttl(name, ttl_millis, *field_names):
    spec = _state_spec(name, *field_names)
    spec.ttl_millis = ttl_millis
    return spec


def _operation(handle):
    operation = object.__new__(ProcessTableFunctionOperation)
    operation._state_specs = [_state_spec("memory", "count")]
    operation._state_handles = [handle]
    operation._context = _ProcessTableFunctionContext(_TimerEmitter())
    operation._context.set_event(1000, 900, 800)
    return operation


class ProcessTableFunctionOperationTests(unittest.TestCase):

    def test_empty_state_is_injected_and_written_after_generator_completion(self):
        handle = _StateHandle()
        operation = _operation(handle)

        def callback(ctx, memory):
            self.assertIsNone(memory.count)
            memory["count"] = 1
            yield Row(1)

        self.assertEqual([[1]], list(operation._invoke(callback, [])))
        self.assertEqual(1, handle.updated[0].count)
        self.assertEqual(0, handle.clear_count)

    def test_explicit_clear_wins_over_state_mutation(self):
        handle = _StateHandle(Row(count=1))
        operation = _operation(handle)

        def callback(ctx, memory):
            memory["count"] = 2
            ctx.clear_state("memory")
            return None

        self.assertEqual([], list(operation._invoke(callback, [])))
        self.assertEqual([], handle.updated)
        self.assertEqual(1, handle.clear_count)

    def test_unknown_state_clear_is_rejected(self):
        context = _ProcessTableFunctionContext(_TimerEmitter(), ["memory"])

        with self.assertRaisesRegex(ValueError, "Unknown state entry: missing"):
            context.clear_state("missing")

    def test_all_null_state_is_cleared(self):
        handle = _StateHandle(Row(count=1))
        operation = _operation(handle)

        def callback(ctx, memory):
            memory["count"] = None
            return None

        self.assertEqual([], list(operation._invoke(callback, [])))
        self.assertEqual(1, handle.clear_count)

    def test_state_is_not_written_when_generator_raises(self):
        handle = _StateHandle(Row(count=1))
        operation = _operation(handle)

        def callback(ctx, memory):
            memory["count"] = 2
            yield Row(2)
            raise RuntimeError("boom")

        with self.assertRaisesRegex(RuntimeError, "boom"):
            list(operation._invoke(callback, []))
        self.assertEqual([], handle.updated)
        self.assertEqual(0, handle.clear_count)

    def test_multiple_states_follow_declaration_order(self):
        first = _StateHandle(Row(count=1))
        second = _StateHandle()
        operation = _operation(first)
        operation._state_specs = [
            _state_spec("first", "count"),
            _state_spec("second", "count"),
        ]
        operation._state_handles = [first, second]

        def callback(ctx, first_state, second_state):
            first_state["count"] += 1
            second_state["count"] = 10
            return None

        self.assertEqual([], list(operation._invoke(callback, [])))
        self.assertEqual(2, first.updated[0].count)
        self.assertEqual(10, second.updated[0].count)

    def test_state_ttl_is_forwarded_to_remote_backend(self):
        backend = _StateBackend()
        operation = object.__new__(ProcessTableFunctionOperation)
        operation.keyed_state_backend = backend
        operation._state_specs = [
            _state_spec_with_ttl("expiring", 1234, "count"),
            _state_spec_with_ttl("persistent", 0, "count"),
        ]
        operation._state_handles = []

        with mock.patch(
                'pyflink.fn_execution.table.process_table_function.from_proto',
                return_value='state-coder'):
            operation._open_state_handles()

        self.assertEqual(["expiring", "persistent"],
                         [request[0] for request in backend.requests])
        self.assertEqual(["state-coder", "state-coder"],
                         [request[1] for request in backend.requests])
        self.assertEqual(1234, backend.requests[0][2].get_ttl().to_milliseconds())
        self.assertIsNone(backend.requests[1][2])
        self.assertEqual(2, len(operation._state_handles))

    def test_zero_to_many_results(self):
        handle = _StateHandle(Row(count=1))
        operation = _operation(handle)

        def no_results(ctx, memory):
            return None

        def many_results(ctx, memory):
            yield Row(1)
            yield Row(2)

        self.assertEqual([], list(operation._invoke(no_results, [])))
        self.assertEqual([[1], [2]], list(operation._invoke(many_results, [])))

    def test_process_timer_sets_key_and_current_timer(self):
        handle = _StateHandle(Row(count=3))
        backend = _KeyedBackend()
        operation = _operation(handle)
        operation.keyed_state_backend = backend

        class Function(object):
            def on_timer(self, ctx, memory):
                yield Row(memory.count, ctx.current_timer())

        operation._function = Function()
        key = Row("user-1")
        results = list(operation.process_timer(
            (TRIGGER, 1200, "timeout", key, 1100, 1150)))

        self.assertEqual(["user-1"], backend.current_key)
        self.assertEqual([[3, "timeout"]], results)
        self.assertEqual(1200, operation._context.time_context(int).time())


class ProcessTableFunctionTimeContextTests(unittest.TestCase):

    def setUp(self):
        self.emitter = _TimerEmitter()
        self.context = _ProcessTableFunctionContext(self.emitter)
        self.context.set_event(1000, 900, 800, "timeout")

    def test_time_context_conversions(self):
        self.assertEqual(1000, self.context.time_context(int).time())
        self.assertEqual(
            1000, self.context.time_context(Instant).time().to_epoch_milli())
        self.assertEqual(
            datetime.datetime(1970, 1, 1, 0, 0, 1),
            self.context.time_context(datetime.datetime).time())
        self.assertEqual("timeout", self.context.current_timer())
        with self.assertRaises(TypeError):
            self.context.time_context(str)

    def test_register_and_clear_timers(self):
        time_context = self.context.time_context(int)
        time_context.register_on_time(1100)
        time_context.register_on_time("timeout", Instant.of_epoch_milli(1200))
        time_context.clear_timer(1100)
        time_context.clear_timer("timeout")
        time_context.clear_all_timers()

        self.assertEqual(
            [
                (REGISTER_ANONYMOUS, 1100, None),
                (REGISTER_NAMED, 1200, "timeout"),
                (DELETE_ANONYMOUS, 1100, None),
                (DELETE_NAMED, None, "timeout"),
                (CLEAR_ALL, None, None),
            ],
            self.emitter.commands)

    def test_rejects_timezone_aware_datetime(self):
        with self.assertRaisesRegex(ValueError, "UTC-naive"):
            self.context.time_context(datetime.datetime).register_on_time(
                datetime.datetime.now(datetime.timezone.utc))


if __name__ == '__main__':
    unittest.main()
