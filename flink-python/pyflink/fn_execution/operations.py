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

import cloudpickle
from apache_beam.runners.common import _OutputProcessor
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker.operations import Operation

from pyflink.fn_execution import flink_fn_execution_pb2
from pyflink.serializers import PickleSerializer

SCALAR_FUNCTION_URN = "flink:transform:scalar_function:v1"


class ScalarFunctionOperation(Operation):
    """
    An operation that will execute ScalarFunctions for each input element.
    """

    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(ScalarFunctionOperation, self).__init__(name, spec, counter_factory, sampler)
        for tag, op_consumers in consumers.items():
            for consumer in op_consumers:
                self.add_receiver(consumer, 0)

        self.variable_dict = {}
        self.scalar_funcs = []
        self.func = self._generate_func(self.spec.serialized_fn)
        for scalar_func in self.scalar_funcs:
            scalar_func.open(None)

    def setup(self):
        super(ScalarFunctionOperation, self).setup()
        self.output_processor = _OutputProcessor(
            window_fn=None,
            main_receivers=self.receivers[0],
            tagged_receivers=None,
            per_element_output_counter=None)

    def start(self):
        with self.scoped_start_state:
            super(ScalarFunctionOperation, self).start()

    def process(self, o):
        self.output_processor.process_outputs(o, [self.func(o.value)])

    def finish(self):
        super(ScalarFunctionOperation, self).finish()

    def needs_finalization(self):
        return False

    def reset(self):
        super(ScalarFunctionOperation, self).reset()

    def teardown(self):
        for scalar_func in self.scalar_funcs:
            scalar_func.close(None)

    def progress_metrics(self):
        metrics = super(ScalarFunctionOperation, self).progress_metrics()
        metrics.processed_elements.measured.output_element_counts.clear()
        tag = None
        receiver = self.receivers[0]
        metrics.processed_elements.measured.output_element_counts[
            str(tag)] = receiver.opcounter.element_counter.value()
        return metrics

    def _generate_func(self, udfs):
        """
        Generates a lambda function based on udfs.
        :param udfs: a list of the proto representation of the Python :class:`ScalarFunction`
        :return: the generated lambda function
        """
        scalar_functions = [self._extract_scalar_function(udf) for udf in udfs]
        return eval('lambda value: [%s]' % ','.join(scalar_functions), self.variable_dict)

    def _extract_scalar_function(self, scalar_function_proto):
        """
        Extracts scalar_function from the proto representation of a
        :class:`ScalarFunction`.

        :param scalar_function_proto: the proto representation of the Python :class:`ScalarFunction`
        """
        def _next_func_num():
            if not hasattr(self, "_func_num"):
                self._func_num = 0
            else:
                self._func_num += 1
            return self._func_num

        scalar_func = cloudpickle.loads(scalar_function_proto.payload)
        func_name = 'f%s' % _next_func_num()
        self.variable_dict[func_name] = scalar_func.eval
        self.scalar_funcs.append(scalar_func)
        func_args = self._extract_scalar_function_args(scalar_function_proto.inputs)
        return "%s(%s)" % (func_name, func_args)

    def _extract_scalar_function_args(self, args):
        args_str = []
        for arg in args:
            if arg.HasField("udf"):
                # for chaining Python UDF input: the input argument is a Python ScalarFunction
                args_str.append(self._extract_scalar_function(arg.udf))
            elif arg.HasField("inputOffset"):
                # the input argument is a column of the input row
                args_str.append("value[%s]" % arg.inputOffset)
            else:
                # the input argument is a constant value
                args_str.append(self._parse_constant_value(arg.inputConstant))
        return ",".join(args_str)

    def _parse_constant_value(self, constant_value):
        j_type = constant_value[0]
        serializer = PickleSerializer()
        pickled_data = serializer.loads(constant_value[1:])
        # the type set contains
        # TINYINT,SMALLINT,INTEGER,BIGINT,FLOAT,DOUBLE,DECIMAL,CHAR,VARCHAR,NULL,BOOLEAN
        # the pickled_data doesn't need to transfer to anther python object
        if j_type == 0:
            parsed_constant_value = pickled_data
        # the type is DATE
        elif j_type == 1:
            parsed_constant_value = \
                datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=pickled_data)
        # the type is TIME
        elif j_type == 2:
            seconds, milliseconds = divmod(pickled_data, 1000)
            minutes, seconds = divmod(seconds, 60)
            hours, minutes = divmod(minutes, 60)
            parsed_constant_value = datetime.time(hours, minutes, seconds, milliseconds * 1000)
        # the type is TIMESTAMP
        elif j_type == 3:
            parsed_constant_value = \
                datetime.datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0) \
                + datetime.timedelta(milliseconds=pickled_data)
        else:
            raise Exception("Unknown type %s, should never happen" % str(j_type))

        def _next_constant_num():
            if not hasattr(self, "_constant_num"):
                self._constant_num = 0
            else:
                self._constant_num += 1
            return self._constant_num

        constant_value_name = 'c%s' % _next_constant_num()
        self.variable_dict[constant_value_name] = parsed_constant_value
        return constant_value_name


@bundle_processor.BeamTransformFactory.register_urn(
    SCALAR_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter.udfs)


def _create_user_defined_function_operation(factory, transform_proto, consumers, udfs_proto,
                                            operation_cls=ScalarFunctionOperation):
    output_tags = list(transform_proto.outputs.keys())
    output_coders = factory.get_output_coders(transform_proto)
    spec = operation_specs.WorkerDoFn(
        serialized_fn=udfs_proto,
        output_tags=output_tags,
        input=None,
        side_inputs=None,
        output_coders=[output_coders[tag] for tag in output_tags])

    return operation_cls(
        transform_proto.unique_name,
        spec,
        factory.counter_factory,
        factory.state_sampler,
        consumers)
