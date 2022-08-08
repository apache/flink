/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.python.embedded;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.AbstractMessageLite;
import pemja.core.object.PyIterator;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link AbstractOneInputEmbeddedPythonFunctionOperator} is responsible for run Python DataStream
 * operators with one input in Embedded Python environment.
 */
@Internal
public abstract class AbstractOneInputEmbeddedPythonFunctionOperator<IN, OUT>
        extends AbstractEmbeddedDataStreamPythonFunctionOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    /** The TypeInformation of the input data. */
    private final TypeInformation<IN> inputTypeInfo;

    private transient PythonTypeUtils.DataConverter<IN, Object> inputDataConverter;

    protected transient long timestamp;

    public AbstractOneInputEmbeddedPythonFunctionOperator(
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<IN> inputTypeInfo,
            TypeInformation<OUT> outputTypeInfo) {
        super(config, pythonFunctionInfo, outputTypeInfo);
        this.inputTypeInfo = Preconditions.checkNotNull(inputTypeInfo);
    }

    @Override
    public void open() throws Exception {
        super.open();

        inputDataConverter =
                PythonTypeUtils.TypeInfoToDataConverter.typeInfoDataConverter(inputTypeInfo);
    }

    @Override
    public void openPythonInterpreter() {
        // function_protos = ...
        // input_coder_info = ...
        // output_coder_info = ...
        // runtime_context = ...
        // function_context = ...
        // job_parameters = ...
        //
        // from pyflink.fn_execution.embedded.operation_utils import
        // create_one_input_user_defined_data_stream_function_from_protos
        //
        // operation = create_one_input_user_defined_data_stream_function_from_protos(
        //     function_protos, input_coder_info, output_coder_info, runtime_context,
        //     function_context, job_parameters)
        // operation.open()

        interpreter.set(
                "function_protos",
                createUserDefinedFunctionsProto().stream()
                        .map(AbstractMessageLite::toByteArray)
                        .collect(Collectors.toList()));

        interpreter.set(
                "input_coder_info",
                ProtoUtils.createRawTypeCoderInfoDescriptorProto(
                                getInputTypeInfo(),
                                FlinkFnApi.CoderInfoDescriptor.Mode.SINGLE,
                                false,
                                null)
                        .toByteArray());
        interpreter.set(
                "output_coder_info",
                ProtoUtils.createRawTypeCoderInfoDescriptorProto(
                                outputTypeInfo,
                                FlinkFnApi.CoderInfoDescriptor.Mode.SINGLE,
                                false,
                                null)
                        .toByteArray());

        interpreter.set("runtime_context", getRuntimeContext());
        interpreter.set("function_context", getFunctionContext());
        interpreter.set("timer_context", getTimerContext());
        interpreter.set("job_parameters", getJobParameters());
        interpreter.set("keyed_state_backend", getKeyedStateBackend());
        interpreter.set("operator_state_backend", getOperatorStateBackend());
        interpreter.set("side_output_context", sideOutputContext);

        interpreter.exec(
                "from pyflink.fn_execution.embedded.operation_utils import create_one_input_user_defined_data_stream_function_from_protos");

        interpreter.exec(
                "operation = create_one_input_user_defined_data_stream_function_from_protos("
                        + "function_protos,"
                        + "input_coder_info,"
                        + "output_coder_info,"
                        + "runtime_context,"
                        + "function_context,"
                        + "timer_context,"
                        + "side_output_context,"
                        + "job_parameters,"
                        + "keyed_state_backend,"
                        + "operator_state_backend)");

        interpreter.invokeMethod("operation", "open");
    }

    @Override
    public void endInput() {
        if (interpreter != null) {
            interpreter.invokeMethod("operation", "close");
        }
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        collector.setTimestamp(element);
        timestamp = element.getTimestamp();

        IN value = element.getValue();
        PyIterator results =
                (PyIterator)
                        interpreter.invokeMethod(
                                "operation",
                                "process_element",
                                inputDataConverter.toExternal(value));

        while (results.hasNext()) {
            OUT result = outputDataConverter.toInternal(results.next());
            collector.collect(result);
        }
        results.close();
    }

    TypeInformation<IN> getInputTypeInfo() {
        return inputTypeInfo;
    }

    /** Gets the proto representation of the Python user-defined functions to be executed. */
    public abstract List<FlinkFnApi.UserDefinedDataStreamFunction>
            createUserDefinedFunctionsProto();

    /** Gets The function context. */
    public abstract Object getFunctionContext();

    /** Gets The Timer context. */
    public abstract Object getTimerContext();
}
