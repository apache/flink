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
from abc import abstractmethod

from apache_beam.runners.worker.operations import Operation
from apache_beam.utils.windowed_value import WindowedValue


class FunctionOperation(Operation):
    """
    Base class of function operation that will execute StatelessFunction or StatefulFunction for
    each input element.
    """

    def __init__(self, name, spec, counter_factory, sampler, consumers, operation_cls):
        super(FunctionOperation, self).__init__(name, spec, counter_factory, sampler)
        self.consumer = consumers['output'][0]
        self._value_coder_impl = self.consumer.windowed_coder.wrapped_value_coder.get_impl()
        self.operation_cls = operation_cls
        self.operation = self.generate_operation()
        self.func = self.operation.func
        self.operation.open()

    def setup(self):
        super(FunctionOperation, self).setup()

    def start(self):
        with self.scoped_start_state:
            super(FunctionOperation, self).start()

    def finish(self):
        with self.scoped_finish_state:
            super(FunctionOperation, self).finish()
            self.operation.finish()

    def needs_finalization(self):
        return False

    def reset(self):
        super(FunctionOperation, self).reset()

    def teardown(self):
        with self.scoped_finish_state:
            self.operation.close()

    def progress_metrics(self):
        metrics = super(FunctionOperation, self).progress_metrics()
        metrics.processed_elements.measured.output_element_counts.clear()
        tag = None
        receiver = self.receivers[0]
        metrics.processed_elements.measured.output_element_counts[
            str(tag)] = receiver.opcounter.element_counter.value()
        return metrics

    def process(self, o: WindowedValue):
        with self.scoped_process_state:
            output_stream = self.consumer.output_stream
            for value in o.value:
                self._value_coder_impl.encode_to_stream(self.func(value), output_stream, True)
                output_stream.maybe_flush()

    def monitoring_infos(self, transform_id, tag_to_pcollection_id):
        """
        Only pass user metric to Java
        :param tag_to_pcollection_id: useless for user metric
        """
        return super().user_monitoring_infos(transform_id)

    @abstractmethod
    def generate_operation(self):
        pass


class StatelessFunctionOperation(FunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers, operation_cls):
        super(StatelessFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers, operation_cls)

    def generate_operation(self):
        return self.operation_cls(self.spec)


class StatefulFunctionOperation(FunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers, operation_cls,
                 keyed_state_backend):
        self.keyed_state_backend = keyed_state_backend
        super(StatefulFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers, operation_cls)

    def generate_operation(self):
        return self.operation_cls(self.spec, self.keyed_state_backend)
