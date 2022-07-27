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
                 input_data_converter: DataConverter,
                 output_data_converter: DataConverter):
        self._operations = operations
        self._input_data_converter = input_data_converter
        self._output_data_converter = output_data_converter

    def open(self):
        for operation in self._operations:
            operation.open()

    def close(self):
        for operation in self._operations:
            operation.close()

    def on_timer(self, timestamp):
        for operation in self._operations:
            for item in operation.on_timer(timestamp):
                yield self._output_data_converter.to_external(item)


class OneInputFunctionOperation(FunctionOperation):
    def __init__(self,
                 serialized_fns,
                 input_data_converter: DataConverter,
                 output_data_converter: DataConverter,
                 runtime_context,
                 function_context,
                 timer_context,
                 job_parameters):
        operations = (
            [extract_process_function(
                serialized_fn,
                runtime_context,
                function_context,
                timer_context,
                job_parameters)
                for serialized_fn in serialized_fns])
        super(OneInputFunctionOperation, self).__init__(
            operations, input_data_converter, output_data_converter)

    def process_element(self, value):
        def _process_element(op, elements):
            for element in elements:
                yield from op.process_element(element)

        value = [self._input_data_converter.to_internal(value)]
        for operation in self._operations:
            value = _process_element(operation, value)

        for item in value:
            yield self._output_data_converter.to_external(item)
