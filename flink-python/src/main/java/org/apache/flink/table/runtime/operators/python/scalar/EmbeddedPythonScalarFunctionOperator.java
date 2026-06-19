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

package org.apache.flink.table.runtime.operators.python.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.python.AbstractEmbeddedStatelessFunctionOperator;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.util.ProtoUtils.createFlattenRowTypeCoderInfoDescriptorProto;

/** The Python {@link ScalarFunction} operator in embedded Python environment. */
@Internal
public class EmbeddedPythonScalarFunctionOperator
        extends AbstractEmbeddedStatelessFunctionOperator {

    private static final long serialVersionUID = 1L;

    /** The Python {@link ScalarFunction}s to be executed. */
    private final PythonFunctionInfo[] scalarFunctions;

    @Nullable private GeneratedProjection forwardedFieldGeneratedProjection;

    /** Whether there is only one input argument. */
    private transient boolean hasOnlyOneInputArgument;

    /** Whether is only one user-defined function. */
    private transient boolean hasOnlyOneUserDefinedFunction;

    /** The Projection which projects the forwarded fields from the input row. */
    private transient Projection<RowData, BinaryRowData> forwardedFieldProjection;

    public EmbeddedPythonScalarFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            int[] udfInputOffsets,
            @Nullable GeneratedProjection forwardedFieldGeneratedProjection) {
        super(config, inputType, udfInputType, udfOutputType, udfInputOffsets);
        this.scalarFunctions = Preconditions.checkNotNull(scalarFunctions);
        this.forwardedFieldGeneratedProjection = forwardedFieldGeneratedProjection;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open() throws Exception {
        hasOnlyOneInputArgument = udfInputOffsets.length == 1;
        hasOnlyOneUserDefinedFunction = udfOutputType.getFieldCount() == 1;

        if (forwardedFieldGeneratedProjection != null) {
            forwardedFieldProjection =
                    forwardedFieldGeneratedProjection.newInstance(
                            Thread.currentThread().getContextClassLoader());
        }

        super.open();
    }

    @Override
    public void openPythonInterpreter() {
        // from pyflink.fn_execution.embedded.operation_utils import
        // create_scalar_operation_from_proto
        //
        // proto = xxx
        // scalar_operation = create_scalar_operation_from_proto(
        //     proto, input_coder_proto, output_coder_proto,
        //     hasOnlyOneInputArgument, hasOnlyOneUserDefinedFunction)
        // scalar_operation.open()

        interpreter.exec(
                "from pyflink.fn_execution.embedded.operation_utils import create_scalar_operation_from_proto");

        interpreter.set(
                "input_coder_proto",
                createFlattenRowTypeCoderInfoDescriptorProto(
                                udfInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false)
                        .toByteArray());

        interpreter.set(
                "output_coder_proto",
                createFlattenRowTypeCoderInfoDescriptorProto(
                                udfOutputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false)
                        .toByteArray());

        // initialize scalar_operation
        interpreter.set(
                "proto",
                ProtoUtils.createUserDefinedFunctionsProto(
                                getRuntimeContext(),
                                scalarFunctions,
                                config.get(PYTHON_METRIC_ENABLED),
                                config.get(PYTHON_PROFILE_ENABLED))
                        .toByteArray());
        interpreter.exec(
                String.format(
                        "scalar_operation = create_scalar_operation_from_proto("
                                + "proto,"
                                + "input_coder_proto,"
                                + "output_coder_proto,"
                                + "%s,"
                                + "%s)",
                        hasOnlyOneInputArgument ? "True" : "False",
                        hasOnlyOneUserDefinedFunction ? "True" : "False"));

        // invoke the open method of scalar_operation which calls
        // the open method of the user-defined functions.
        interpreter.invokeMethod("scalar_operation", "open");
    }

    @Override
    public void endInput() {
        if (interpreter != null) {
            // invoke the close method of scalar_operation  which calls
            // the close method of the user-defined functions.
            interpreter.invokeMethod("scalar_operation", "close");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void processElement(StreamRecord<RowData> element) {
        RowData value = element.getValue();

        Object udfArgs = null;
        if (userDefinedFunctionInputArgs.length > 1) {
            for (int i = 0; i < userDefinedFunctionInputArgs.length; i++) {
                userDefinedFunctionInputArgs[i] =
                        userDefinedFunctionInputConverters[i].toExternal(value, udfInputOffsets[i]);
            }
            udfArgs = userDefinedFunctionInputArgs;
        } else if (userDefinedFunctionInputArgs.length == 1) {
            udfArgs = userDefinedFunctionInputConverters[0].toExternal(value, udfInputOffsets[0]);
        }

        if (hasOnlyOneUserDefinedFunction) {
            Object udfResult =
                    interpreter.invokeMethod("scalar_operation", "process_element", udfArgs);
            reuseResultRowData.setField(
                    0, userDefinedFunctionOutputConverters[0].toInternal(udfResult));
        } else {
            Object[] udfResult =
                    (Object[])
                            interpreter.invokeMethod(
                                    "scalar_operation", "process_element", udfArgs);
            for (int i = 0; i < udfResult.length; i++) {
                reuseResultRowData.setField(
                        i, userDefinedFunctionOutputConverters[i].toInternal(udfResult[i]));
            }
        }

        if (forwardedFieldProjection != null) {
            BinaryRowData forwardedRowData = forwardedFieldProjection.apply(value).copy();
            JoinedRowData reuseJoinedRow = new JoinedRowData(forwardedRowData, reuseResultRowData);
            rowDataWrapper.collect(reuseJoinedRow);
        } else {
            rowDataWrapper.collect(reuseResultRowData);
        }
    }
}
