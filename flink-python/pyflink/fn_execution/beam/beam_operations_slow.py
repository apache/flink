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
from abc import abstractmethod
from typing import Iterable, Any, Dict, List

from apache_beam.runners.worker.bundle_processor import TimerInfo, DataOutputOperation
from apache_beam.runners.worker.operations import Operation
from apache_beam.utils import windowed_value
from apache_beam.utils.windowed_value import WindowedValue

from pyflink.common.constants import DEFAULT_OUTPUT_TAG
from pyflink.fn_execution.flink_fn_execution_pb2 import UserDefinedDataStreamFunction
from pyflink.fn_execution.table.operations import BundleOperation
from pyflink.fn_execution.profiler import Profiler


class OutputProcessor(abc.ABC):

    @abstractmethod
    def process_outputs(self, windowed_value: WindowedValue, results: Iterable[Any]):
        pass

    def close(self):
        pass


class NetworkOutputProcessor(OutputProcessor):

    def __init__(self, consumer):
        assert isinstance(consumer, DataOutputOperation)
        self._consumer = consumer
        self._value_coder_impl = consumer.windowed_coder.wrapped_value_coder.get_impl()._value_coder

    def process_outputs(self, windowed_value: WindowedValue, results: Iterable[Any]):
        output_stream = self._consumer.output_stream
        self._value_coder_impl.encode_to_stream(results, output_stream, True)
        self._value_coder_impl._output_stream.maybe_flush()

    def close(self):
        self._value_coder_impl._output_stream.close()


class IntermediateOutputProcessor(OutputProcessor):

    def __init__(self, consumer):
        self._consumer = consumer

    def process_outputs(self, windowed_value: WindowedValue, results: Iterable[Any]):
        self._consumer.process(windowed_value.with_value(results))


class FunctionOperation(Operation):
    """
    Base class of function operation that will execute StatelessFunction or StatefulFunction for
    each input element.
    """

    def __init__(self, name, spec, counter_factory, sampler, consumers, operation_cls,
                 operator_state_backend):
        super(FunctionOperation, self).__init__(name, spec, counter_factory, sampler)
        self._output_processors = self._create_output_processors(
            consumers
        )  # type: Dict[str, List[OutputProcessor]]
        self.operation_cls = operation_cls
        self.operator_state_backend = operator_state_backend
        self.operation = self.generate_operation()
        self.process_element = self.operation.process_element
        self.operation.open()
        if spec.serialized_fn.profile_enabled:
            self._profiler = Profiler()
        else:
            self._profiler = None

        if isinstance(spec.serialized_fn, UserDefinedDataStreamFunction):
            self._has_side_output = spec.serialized_fn.has_side_output
        else:
            # it doesn't support side output in Table API & SQL
            self._has_side_output = False
        if not self._has_side_output:
            self._main_output_processor = self._output_processors[DEFAULT_OUTPUT_TAG][0]

    def setup(self):
        super(FunctionOperation, self).setup()

    def start(self):
        with self.scoped_start_state:
            super(FunctionOperation, self).start()
            if self._profiler:
                self._profiler.start()

    def finish(self):
        with self.scoped_finish_state:
            super(FunctionOperation, self).finish()
            self.operation.finish()
            if self._profiler:
                self._profiler.close()

    def needs_finalization(self):
        return False

    def reset(self):
        super(FunctionOperation, self).reset()

    def teardown(self):
        with self.scoped_finish_state:
            self.operation.close()
            for processors in self._output_processors.values():
                for p in processors:
                    p.close()

    def progress_metrics(self):
        metrics = super(FunctionOperation, self).progress_metrics()
        metrics.processed_elements.measured.output_element_counts.clear()
        tag = None
        receiver = self.receivers[0]
        metrics.processed_elements.measured.output_element_counts[
            str(tag)
        ] = receiver.opcounter.element_counter.value()
        return metrics

    def process(self, o: WindowedValue):
        with self.scoped_process_state:
            if self._has_side_output:
                for value in o.value:
                    for tag, row in self.process_element(value):
                        for p in self._output_processors.get(tag, []):
                            p.process_outputs(o, [row])
            else:
                if isinstance(self.operation, BundleOperation):
                    for value in o.value:
                        self.process_element(value)
                    self._main_output_processor.process_outputs(o, self.operation.finish_bundle())
                else:
                    for value in o.value:
                        self._main_output_processor.process_outputs(
                            o, self.operation.process_element(value)
                        )

    def monitoring_infos(self, transform_id, tag_to_pcollection_id):
        """
        Only pass user metric to Java
        :param tag_to_pcollection_id: useless for user metric
        """
        return super().user_monitoring_infos(transform_id)

    @staticmethod
    def _create_output_processors(consumers_map):
        def _create_processor(consumer):
            if isinstance(consumer, DataOutputOperation):
                return NetworkOutputProcessor(consumer)
            else:
                return IntermediateOutputProcessor(consumer)

        return {
            tag: [_create_processor(c) for c in consumers]
            for tag, consumers in consumers_map.items()
        }

    @abstractmethod
    def generate_operation(self):
        pass


class StatelessFunctionOperation(FunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers, operation_cls,
                 operator_state_backend):
        super(StatelessFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers, operation_cls, operator_state_backend
        )

    def generate_operation(self):
        if self.operator_state_backend is not None:
            return self.operation_cls(self.spec.serialized_fn, self.operator_state_backend)
        else:
            return self.operation_cls(self.spec.serialized_fn)


class StatefulFunctionOperation(FunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers, operation_cls,
                 keyed_state_backend, operator_state_backend):
        self._keyed_state_backend = keyed_state_backend
        self._reusable_windowed_value = windowed_value.create(None, -1, None, None)
        super(StatefulFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers, operation_cls, operator_state_backend
        )

    def generate_operation(self):
        if self.operator_state_backend is not None:
            return self.operation_cls(self.spec.serialized_fn, self._keyed_state_backend,
                                      self.operator_state_backend)
        else:
            return self.operation_cls(self.spec.serialized_fn, self._keyed_state_backend)

    def add_timer_info(self, timer_family_id: str, timer_info: TimerInfo):
        # ignore timer_family_id
        self.operation.add_timer_info(timer_info)

    def process_timer(self, tag, timer_data):
        if self._has_side_output:
            # the field user_key holds the timer data
            for tag, row in self.operation.process_timer(timer_data.user_key):
                for p in self._output_processors.get(tag, []):
                    p.process_outputs(self._reusable_windowed_value, [row])
        else:
            self._main_output_processor.process_outputs(
                self._reusable_windowed_value,
                # the field user_key holds the timer data
                self.operation.process_timer(timer_data.user_key),
            )
