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
from abc import abstractmethod, ABCMeta

from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker.operations import Operation

from pyflink.fn_execution import flink_fn_execution_pb2
from pyflink.serializers import PickleSerializer

SCALAR_FUNCTION_URN = "flink:transform:scalar_function:v1"


class InputGetter(object):
    """
    Base class for get an input argument for a :class:`UserDefinedFunction`.
    """
    __metaclass__ = ABCMeta

    def open(self):
        pass

    def close(self):
        pass

    @abstractmethod
    def get(self, value):
        pass


class OffsetInputGetter(InputGetter):
    """
    InputGetter for the input argument which is a column of the input row.

    :param input_offset: the offset of the column in the input row
    """

    def __init__(self, input_offset):
        self.input_offset = input_offset

    def get(self, value):
        return value[self.input_offset]


class ScalarFunctionInputGetter(InputGetter):
    """
    InputGetter for the input argument which is a Python :class:`ScalarFunction`. This is used for
    chaining Python functions.

    :param scalar_function_proto: the proto representation of the Python :class:`ScalarFunction`
    """

    def __init__(self, scalar_function_proto):
        self.scalar_function_invoker = create_scalar_function_invoker(scalar_function_proto)

    def open(self):
        self.scalar_function_invoker.invoke_open()

    def close(self):
        self.scalar_function_invoker.invoke_close()

    def get(self, value):
        return self.scalar_function_invoker.invoke_eval(value)


class ConstantInputGetter(InputGetter):
    """
    InputGetter for the input argument which is a constant value.

    :param constant_value: the constant value of the column
    """

    def __init__(self, constant_value):
        j_type = constant_value[0]
        serializer = PickleSerializer()
        pickled_data = serializer.loads(constant_value[1:])
        # the type set contains
        # TINYINT,SMALLINT,INTEGER,BIGINT,FLOAT,DOUBLE,DECIMAL,CHAR,VARCHAR,NULL,BOOLEAN
        # the pickled_data doesn't need to transfer to anther python object
        if j_type == 0:
            self._constant_value = pickled_data
        # the type is DATE
        elif j_type == 1:
            self._constant_value = \
                datetime.date(year=1970, month=1, day=1) + datetime.timedelta(days=pickled_data)
        # the type is TIME
        elif j_type == 2:
            seconds, milliseconds = divmod(pickled_data, 1000)
            minutes, seconds = divmod(seconds, 60)
            hours, minutes = divmod(minutes, 60)
            self._constant_value = datetime.time(hours, minutes, seconds, milliseconds * 1000)
        # the type is TIMESTAMP
        elif j_type == 3:
            self._constant_value = \
                datetime.datetime(year=1970, month=1, day=1, hour=0, minute=0, second=0) \
                + datetime.timedelta(milliseconds=pickled_data)
        else:
            raise Exception("Unknown type %s, should never happen" % str(j_type))

    def get(self, value):
        return self._constant_value


class ScalarFunctionInvoker(object):
    """
    An abstraction that can be used to execute :class:`ScalarFunction` methods.

    A ScalarFunctionInvoker describes a particular way for invoking methods of a
    :class:`ScalarFunction`.

    :param scalar_function: the :class:`ScalarFunction` to execute
    :param inputs: the input arguments for the :class:`ScalarFunction`
    """

    def __init__(self, scalar_function, inputs):
        self.scalar_function = scalar_function
        self.input_getters = []
        for input in inputs:
            if input.HasField("udf"):
                # for chaining Python UDF input: the input argument is a Python ScalarFunction
                self.input_getters.append(ScalarFunctionInputGetter(input.udf))
            elif input.HasField("inputOffset"):
                # the input argument is a column of the input row
                self.input_getters.append(OffsetInputGetter(input.inputOffset))
            else:
                # the input argument is a constant value
                self.input_getters.append(ConstantInputGetter(input.inputConstant))

    def invoke_open(self):
        """
        Invokes the ScalarFunction.open() function.
        """
        for input_getter in self.input_getters:
            input_getter.open()
        # set the FunctionContext to None for now
        self.scalar_function.open(None)

    def invoke_close(self):
        """
        Invokes the ScalarFunction.close() function.
        """
        for input_getter in self.input_getters:
            input_getter.close()
        self.scalar_function.close()

    def invoke_eval(self, value):
        """
        Invokes the ScalarFunction.eval() function.

        :param value: the input element for which eval() method should be invoked
        """
        args = [input_getter.get(value) for input_getter in self.input_getters]
        return self.scalar_function.eval(*args)


def create_scalar_function_invoker(scalar_function_proto):
    """
    Creates :class:`ScalarFunctionInvoker` from the proto representation of a
    :class:`ScalarFunction`.

    :param scalar_function_proto: the proto representation of the Python :class:`ScalarFunction`
    :return: :class:`ScalarFunctionInvoker`.
    """
    import cloudpickle
    scalar_function = cloudpickle.loads(scalar_function_proto.payload)
    return ScalarFunctionInvoker(scalar_function, scalar_function_proto.inputs)


class ScalarFunctionRunner(object):
    """
    The runner which is responsible for executing the scalar functions and send the
    execution results back to the remote Java operator.

    :param udfs_proto: protocol representation for the scalar functions to execute
    """

    def __init__(self, udfs_proto):
        self.scalar_function_invokers = [create_scalar_function_invoker(f) for f in
                                         udfs_proto]

    def setup(self, main_receivers):
        """
        Set up the ScalarFunctionRunner.

        :param main_receivers: Receiver objects which is responsible for sending the execution
                               results back the the remote Java operator
        """
        from apache_beam.runners.common import _OutputProcessor
        self.output_processor = _OutputProcessor(
            window_fn=None,
            main_receivers=main_receivers,
            tagged_receivers=None,
            per_element_output_counter=None)

    def open(self):
        for invoker in self.scalar_function_invokers:
            invoker.invoke_open()

    def close(self):
        for invoker in self.scalar_function_invokers:
            invoker.invoke_close()

    def process(self, windowed_value):
        results = [invoker.invoke_eval(windowed_value.value) for invoker in
                   self.scalar_function_invokers]
        from pyflink.table import Row
        result = Row(*results)
        # send the execution results back
        self.output_processor.process_outputs(windowed_value, [result])


class ScalarFunctionOperation(Operation):
    """
    An operation that will execute ScalarFunctions for each input element.
    """

    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(ScalarFunctionOperation, self).__init__(name, spec, counter_factory, sampler)
        for tag, op_consumers in consumers.items():
            for consumer in op_consumers:
                self.add_receiver(consumer, 0)

        self.scalar_function_runner = ScalarFunctionRunner(self.spec.serialized_fn)
        self.scalar_function_runner.open()

    def setup(self):
        with self.scoped_start_state:
            super(ScalarFunctionOperation, self).setup()
            self.scalar_function_runner.setup(self.receivers[0])

    def start(self):
        with self.scoped_start_state:
            super(ScalarFunctionOperation, self).start()

    def process(self, o):
        with self.scoped_process_state:
            self.scalar_function_runner.process(o)

    def finish(self):
        with self.scoped_finish_state:
            super(ScalarFunctionOperation, self).finish()

    def needs_finalization(self):
        return False

    def reset(self):
        super(ScalarFunctionOperation, self).reset()

    def teardown(self):
        with self.scoped_finish_state:
            self.scalar_function_runner.close()

    def progress_metrics(self):
        metrics = super(ScalarFunctionOperation, self).progress_metrics()
        metrics.processed_elements.measured.output_element_counts.clear()
        tag = None
        receiver = self.receivers[0]
        metrics.processed_elements.measured.output_element_counts[
            str(tag)] = receiver.opcounter.element_counter.value()
        return metrics


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
