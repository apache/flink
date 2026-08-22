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

from pyflink.common import Instant, Row, Time
from pyflink.datastream.state import StateTtlConfig
from pyflink.fn_execution import pickle
from pyflink.fn_execution.coders import from_proto
from pyflink.fn_execution.table.operations import BaseOperation
from pyflink.fn_execution.utils.operation_utils import normalize_table_function_result


PROCESS_TABLE_FUNCTION_URN = "flink:transform:process_table_function:v1"

REGISTER_ANONYMOUS = 0
REGISTER_NAMED = 1
DELETE_ANONYMOUS = 2
DELETE_NAMED = 3
CLEAR_ALL = 4
TRIGGER = 5


def _state_key(key):
    return list(key) if isinstance(key, Row) else key


def _to_millis(value):
    if isinstance(value, bool):
        raise TypeError("A timer timestamp must not be a bool.")
    if isinstance(value, int):
        return value
    if isinstance(value, Instant):
        return value.to_epoch_milli()
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            raise ValueError("Timer datetime values must be UTC-naive.")
        epoch = datetime.datetime(1970, 1, 1)
        return int((value - epoch).total_seconds() * 1000)
    raise TypeError("Time values must be int, Instant, or UTC-naive datetime.datetime.")


def _from_millis(value, conversion_type):
    if value is None:
        return None
    if conversion_type is int:
        return value
    if conversion_type is Instant:
        return Instant.of_epoch_milli(value)
    if conversion_type is datetime.datetime:
        return datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=value)
    raise TypeError("Time context type must be int, Instant, or datetime.datetime.")


class _TimerCommandEmitter(object):

    def __init__(self, keyed_state_backend):
        self._keyed_state_backend = keyed_state_backend
        self._timer_coder_impl = None
        self._output_stream = None

    def add_timer_info(self, timer_info):
        self._timer_coder_impl = timer_info.timer_coder_impl
        self._output_stream = timer_info.output_stream

    def emit(self, operation, timestamp=None, name=None):
        if self._timer_coder_impl is None:
            raise RuntimeError("Timer services are not available for this PTF call.")
        from apache_beam.transforms import userstate
        from apache_beam.transforms.window import GlobalWindow

        key = self._keyed_state_backend.get_current_key()
        timer_key = Row(*key) if isinstance(key, list) else key
        timer_data = Row(operation, timestamp, name, timer_key, None, None)
        timer = userstate.Timer(
            user_key=timer_data,
            dynamic_timer_tag='',
            windows=(GlobalWindow(),),
            clear_bit=True,
            fire_timestamp=None,
            hold_timestamp=None,
            paneinfo=None)
        self._timer_coder_impl.encode_to_stream(timer, self._output_stream, True)
        self._timer_coder_impl._key_coder_impl._value_coder._output_stream.maybe_flush()


class _ProcessTableTimeContext(object):

    def __init__(self, context, conversion_type):
        if conversion_type not in (int, Instant, datetime.datetime):
            raise TypeError("Time context type must be int, Instant, or datetime.datetime.")
        self._context = context
        self._conversion_type = conversion_type

    def time(self):
        return _from_millis(self._context._time, self._conversion_type)

    def table_watermark(self):
        return _from_millis(self._context._table_watermark, self._conversion_type)

    def current_watermark(self):
        return _from_millis(self._context._current_watermark, self._conversion_type)

    def register_on_time(self, *args):
        if len(args) == 1:
            self._context._timer_emitter.emit(REGISTER_ANONYMOUS, _to_millis(args[0]))
        elif len(args) == 2 and isinstance(args[0], str):
            self._context._timer_emitter.emit(
                REGISTER_NAMED, _to_millis(args[1]), args[0])
        else:
            raise TypeError("register_on_time expects time or name, time.")

    def clear_timer(self, time_or_name):
        if isinstance(time_or_name, str):
            self._context._timer_emitter.emit(DELETE_NAMED, name=time_or_name)
        else:
            self._context._timer_emitter.emit(
                DELETE_ANONYMOUS, timestamp=_to_millis(time_or_name))

    def clear_all_timers(self):
        self._context.clear_all_timers()


class _ProcessTableFunctionContext(object):

    def __init__(self, timer_emitter, state_names=None):
        self._timer_emitter = timer_emitter
        self._state_names = None if state_names is None else frozenset(state_names)
        self._cleared_states = set()
        self._time = None
        self._table_watermark = None
        self._current_watermark = None
        self._current_timer = None

    def set_event(self, time, table_watermark, current_watermark, current_timer=None):
        self._cleared_states.clear()
        self._time = time
        self._table_watermark = table_watermark
        self._current_watermark = current_watermark
        self._current_timer = current_timer

    def time_context(self, conversion_type):
        return _ProcessTableTimeContext(self, conversion_type)

    def clear_state(self, name):
        if self._state_names is not None and name not in self._state_names:
            raise ValueError("Unknown state entry: %s" % name)
        self._cleared_states.add(name)

    def clear_all_state(self):
        self._cleared_states.add(None)

    def clear_all_timers(self):
        self._timer_emitter.emit(CLEAR_ALL)

    def clear_all(self):
        self.clear_all_state()
        self.clear_all_timers()

    def current_timer(self):
        return self._current_timer


class ProcessTableFunctionOperation(BaseOperation):

    def __init__(self, serialized_fn, keyed_state_backend=None):
        self.keyed_state_backend = keyed_state_backend
        self._function = pickle.loads(serialized_fn.payload)
        self._state_specs = list(serialized_fn.states)
        self._state_handles = []
        self._timer_emitter = _TimerCommandEmitter(keyed_state_backend)
        self._context = _ProcessTableFunctionContext(
            self._timer_emitter, [state.name for state in self._state_specs])
        super(ProcessTableFunctionOperation, self).__init__(serialized_fn)

    def generate_func(self, serialized_fn):
        return lambda value: (), [self._function]

    def open(self):
        super(ProcessTableFunctionOperation, self).open()
        if self.keyed_state_backend is None:
            return
        self._open_state_handles()

    def _open_state_handles(self):
        for state in self._state_specs:
            ttl_config = None
            if state.ttl_millis > 0:
                ttl_config = StateTtlConfig.new_builder(
                    Time.milliseconds(state.ttl_millis)).build()
            self._state_handles.append(self.keyed_state_backend.get_value_state(
                state.name, from_proto(state.type), ttl_config))

    def finish(self):
        super(ProcessTableFunctionOperation, self).finish()
        if self.keyed_state_backend is not None:
            self.keyed_state_backend.commit()

    def add_timer_info(self, timer_info):
        self._timer_emitter.add_timer_info(timer_info)

    def process_element(self, value):
        key, arguments, time, table_watermark, current_watermark = value
        if self.keyed_state_backend is not None:
            self.keyed_state_backend.set_current_key(_state_key(key))
        self._context.set_event(time, table_watermark, current_watermark)
        return self._invoke(self._function.eval, list(arguments))

    def process_timer(self, timer_data):
        operation, timestamp, name, key, table_watermark, current_watermark = timer_data
        if operation != TRIGGER:
            raise ValueError("Unexpected PTF timer input operation: %s" % operation)
        self.keyed_state_backend.set_current_key(_state_key(key))
        self._context.set_event(timestamp, table_watermark, current_watermark, name)
        return self._invoke(self._function.on_timer, [])

    def _invoke(self, callback, arguments):
        states = self._read_states()

        def invoke():
            completed = False
            try:
                results = callback(self._context, *states, *arguments)
                for result in normalize_table_function_result(results):
                    yield result
                completed = True
            finally:
                if completed:
                    self._write_states(states)

        return invoke()

    def _read_states(self):
        states = []
        for spec, handle in zip(self._state_specs, self._state_handles):
            value = handle.value()
            if value is None:
                value = Row(**{field.name: None for field in spec.type.row_schema.fields})
            states.append(value)
        return states

    def _write_states(self, states):
        clear_all = None in self._context._cleared_states
        for spec, handle, value in zip(self._state_specs, self._state_handles, states):
            if clear_all or spec.name in self._context._cleared_states or all(
                    field is None for field in value._values):
                handle.clear()
            else:
                handle.update(value)
