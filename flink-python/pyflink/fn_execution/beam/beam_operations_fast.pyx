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
# cython: language_level = 3
# cython: infer_types = True
# cython: profile=True
# cython: boundscheck=False, wraparound=False, initializedcheck=False, cdivision=True
from libc.stdint cimport *
from apache_beam.utils.windowed_value cimport WindowedValue

from pyflink.fn_execution.coder_impl_fast cimport BaseCoderImpl
from pyflink.fn_execution.beam.beam_stream cimport BeamInputStream, BeamOutputStream
from pyflink.fn_execution.beam.beam_coder_impl_fast cimport InputStreamWrapper

cdef class FunctionOperation(Operation):
    """
    Base class of function operation that will execute StatelessFunction or StatefulFunction for
    each input element.
    """

    def __init__(self, name, spec, counter_factory, sampler, consumers, operation_cls):
        super(FunctionOperation, self).__init__(name, spec, counter_factory, sampler)
        self.consumer = consumers['output'][0]
        self._value_coder_impl = self.consumer.windowed_coder.wrapped_value_coder.get_impl()._value_coder
        from pyflink.fn_execution.beam.beam_coder_impl_slow import ArrowCoderImpl, \
            OverWindowArrowCoderImpl

        if isinstance(self._value_coder_impl, (ArrowCoderImpl, OverWindowArrowCoderImpl)):
            self._is_python_coder = True
        else:
            self._is_python_coder = False
            self._output_coder = self._value_coder_impl._value_coder

        self.operation_cls = operation_cls
        self.operation = self.generate_operation()
        self.process_element = self.operation.process_element
        self.operation.open()

    cpdef start(self):
        with self.scoped_start_state:
            super(FunctionOperation, self).start()

    cpdef finish(self):
        with self.scoped_finish_state:
            super(FunctionOperation, self).finish()
            self.operation.finish()

    cpdef teardown(self):
        with self.scoped_finish_state:
            self.operation.close()

    cpdef process(self, WindowedValue o):
        cdef InputStreamWrapper input_stream_wrapper
        cdef BeamInputStream input_stream
        cdef BaseCoderImpl input_coder
        cdef BeamOutputStream output_stream
        with self.scoped_process_state:
            if self._is_python_coder:
                for value in o.value:
                    self._value_coder_impl.encode_to_stream(
                        self.process_element(value), self.consumer.output_stream, True)
                    self.consumer.output_stream.maybe_flush()
            else:
                input_stream_wrapper = o.value
                input_stream = input_stream_wrapper._input_stream
                input_coder = input_stream_wrapper._value_coder
                output_stream = BeamOutputStream(self.consumer.output_stream)
                while input_stream.available():
                    input_data = input_coder.decode_from_stream(input_stream)
                    result = self.process_element(input_data)
                    self._output_coder.encode_to_stream(result, output_stream)
                output_stream.flush()

    def progress_metrics(self):
        metrics = super(FunctionOperation, self).progress_metrics()
        metrics.processed_elements.measured.output_element_counts.clear()
        tag = None
        receiver = self.receivers[0]
        metrics.processed_elements.measured.output_element_counts[
            str(tag)] = receiver.opcounter.element_counter.value()
        return metrics

    cpdef monitoring_infos(self, transform_id, tag_to_pcollection_id):
        """
        Only pass user metric to Java
        :param tag_to_pcollection_id: useless for user metric
        """
        return self.user_monitoring_infos(transform_id)

    cdef object generate_operation(self):
        pass


cdef class StatelessFunctionOperation(FunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers, operation_cls):
        super(StatelessFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers, operation_cls)

    cdef object generate_operation(self):
        return self.operation_cls(self.spec)


cdef class StatefulFunctionOperation(FunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers, operation_cls,
                 keyed_state_backend):
        self.keyed_state_backend = keyed_state_backend
        super(StatefulFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers, operation_cls)

    cdef object generate_operation(self):
        return self.operation_cls(self.spec, self.keyed_state_backend)
