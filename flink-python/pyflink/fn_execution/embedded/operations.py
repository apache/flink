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
from pyflink.fn_execution.datastream.embedded.operations import extract_process_function
from pyflink.fn_execution.embedded.converters import DataConverter


class FunctionOperation(object):
    def __init__(self,
                 operations,
                 output_data_converter: DataConverter):
        self._operations = operations
        self._output_data_converter = output_data_converter
        self._main_operation = operations[0]
        self._chained_operations = operations[1:]

    def open(self):
        for operation in self._operations:
            operation.open()

    def close(self):
        for operation in self._operations:
            operation.close()

    def on_timer(self, timestamp):
        results = self._main_operation.on_timer(timestamp)

        if results:
            results = self._process_elements(results)
            yield from self._output_elements(results)

    def _process_elements(self, elements):
        def _process_elements_on_operation(op, items):
            for item in items:
                yield from op.process_element(item)

        for operation in self._chained_operations:
            elements = _process_elements_on_operation(operation, elements)

        return elements

    def _output_elements(self, elements):
        for item in elements:
            yield self._output_data_converter.to_external(item)


class OneInputFunctionOperation(FunctionOperation):
    def __init__(self,
                 serialized_fns,
                 input_data_converter: DataConverter,
                 output_data_converter: DataConverter,
                 runtime_context,
                 function_context,
                 timer_context,
                 side_output_context,
                 job_parameters,
                 keyed_state_backend,
                 operator_state_backend):
        operations = (
            [extract_process_function(
                serialized_fn,
                runtime_context,
                function_context,
                timer_context,
                side_output_context,
                job_parameters,
                keyed_state_backend,
                operator_state_backend)
                for serialized_fn in serialized_fns])
        super(OneInputFunctionOperation, self).__init__(operations, output_data_converter)
        self._input_data_converter = input_data_converter

    def process_element(self, value):
        results = self._main_operation.process_element(
            self._input_data_converter.to_internal(value))

        results = self._process_elements(results)

        yield from self._output_elements(results)


class TwoInputFunctionOperation(FunctionOperation):
    def __init__(self,
                 serialized_fns,
                 input_data_converter1: DataConverter,
                 input_data_converter2: DataConverter,
                 output_data_converter: DataConverter,
                 runtime_context,
                 function_context,
                 timer_context,
                 side_output_context,
                 job_parameters,
                 keyed_state_backend,
                 operator_state_backend):
        operations = (
            [extract_process_function(
                serialized_fn,
                runtime_context,
                function_context,
                timer_context,
                side_output_context,
                job_parameters,
                keyed_state_backend,
                operator_state_backend)
                for serialized_fn in serialized_fns])
        super(TwoInputFunctionOperation, self).__init__(operations, output_data_converter)
        self._input_data_converter1 = input_data_converter1
        self._input_data_converter2 = input_data_converter2
        self._main_operation = operations[0]
        self._other_operations = operations[1:]

    def process_element1(self, value):
        results = self._main_operation.process_element1(
            self._input_data_converter1.to_internal(value))

        results = self._process_elements(results)

        yield from self._output_elements(results)

    def process_element2(self, value):
        results = self._main_operation.process_element2(
            self._input_data_converter2.to_internal(value))

        results = self._process_elements(results)

        yield from self._output_elements(results)
