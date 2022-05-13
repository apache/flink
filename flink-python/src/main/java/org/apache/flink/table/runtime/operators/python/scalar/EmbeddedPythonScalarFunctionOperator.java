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
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.python.AbstractEmbeddedPythonFunctionOperator;
import org.apache.flink.streaming.api.utils.ProtoUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;

/** The Python {@link ScalarFunction} operator in embedded Python environment. */
@Internal
public class EmbeddedPythonScalarFunctionOperator
        extends AbstractEmbeddedPythonFunctionOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    /** The Python {@link ScalarFunction}s to be executed. */
    private final PythonFunctionInfo[] scalarFunctions;

    /** The offsets of user-defined function inputs. */
    private final int[] udfInputOffsets;

    /** The input logical type. */
    protected final RowType inputType;

    /** The user-defined function input logical type. */
    protected final RowType udfInputType;

    /** The user-defined function output logical type. */
    protected final RowType udfOutputType;

    private GeneratedProjection forwardedFieldGeneratedProjection;

    /** The GenericRowData reused holding the execution result of python udf. */
    private GenericRowData reuseResultRowData;

    /** The collector used to collect records. */
    private transient StreamRecordRowDataWrappingCollector rowDataWrapper;

    /** The Projection which projects the forwarded fields from the input row. */
    private transient Projection<RowData, BinaryRowData> forwardedFieldProjection;

    private transient PythonTypeUtils.DataConverter[] userDefinedFunctionInputConverters;
    private transient Object[] userDefinedFunctionInputArgs;
    private transient PythonTypeUtils.DataConverter[] userDefinedFunctionOutputConverters;

    /** Whether there is only one input argument. */
    private transient boolean isOneArg;

    /** Whether is only one field of udf result. */
    private transient boolean isOneFieldResult;

    public EmbeddedPythonScalarFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            int[] udfInputOffsets) {
        super(config);
        this.inputType = Preconditions.checkNotNull(inputType);
        this.udfInputType = Preconditions.checkNotNull(udfInputType);
        this.udfOutputType = Preconditions.checkNotNull(udfOutputType);
        this.udfInputOffsets = Preconditions.checkNotNull(udfInputOffsets);
        this.scalarFunctions = Preconditions.checkNotNull(scalarFunctions);
    }

    public EmbeddedPythonScalarFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            int[] udfInputOffsets,
            GeneratedProjection forwardedFieldGeneratedProjection) {
        this(config, scalarFunctions, inputType, udfInputType, udfOutputType, udfInputOffsets);
        this.forwardedFieldGeneratedProjection =
                Preconditions.checkNotNull(forwardedFieldGeneratedProjection);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open() throws Exception {
        isOneArg = udfInputOffsets.length == 1;
        isOneFieldResult = udfOutputType.getFieldCount() == 1;
        super.open();
        rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
        reuseResultRowData = new GenericRowData(udfOutputType.getFieldCount());
        RowType userDefinedFunctionInputType =
                new RowType(
                        Arrays.stream(udfInputOffsets)
                                .mapToObj(i -> inputType.getFields().get(i))
                                .collect(Collectors.toList()));
        userDefinedFunctionInputConverters =
                userDefinedFunctionInputType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(PythonTypeUtils::toDataConverter)
                        .toArray(PythonTypeUtils.DataConverter[]::new);
        userDefinedFunctionInputArgs = new Object[udfInputOffsets.length];
        userDefinedFunctionOutputConverters =
                udfOutputType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(PythonTypeUtils::toDataConverter)
                        .toArray(PythonTypeUtils.DataConverter[]::new);

        if (forwardedFieldGeneratedProjection != null) {
            forwardedFieldProjection =
                    forwardedFieldGeneratedProjection.newInstance(
                            Thread.currentThread().getContextClassLoader());
        }
    }

    @Override
    public void openPythonInterpreter(String pythonExecutable, Map<String, String> env) {
        LOG.info("Create Operation in multi-threads.");

        // The CPython extension included in proto does not support initialization
        // multiple times, so we choose the only interpreter process to be responsible for
        // initialization and proto parsing. The only interpreter parses the proto and
        // serializes function operations with cloudpickle.
        interpreter.exec(
                "from pyflink.fn_execution.utils.operation_utils import create_scalar_operation_from_proto");
        interpreter.set("proto", getUserDefinedFunctionsProto().toByteArray());

        interpreter.exec(
                String.format(
                        "scalar_operation = create_scalar_operation_from_proto(proto, %s, %s)",
                        isOneArg ? "True" : "False", isOneFieldResult ? "True" : "False"));

        // invoke `open` method of ScalarOperation.
        interpreter.invokeMethod("scalar_operation", "open");
    }

    @Override
    public void endInput() {
        if (interpreter != null) {
            // invoke `close` method of ScalarOperation.
            interpreter.invokeMethod("scalar_operation", "close");
        }
    }

    @Override
    public PythonEnv getPythonEnv() {
        return scalarFunctions[0].getPythonFunction().getPythonEnv();
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

        if (isOneFieldResult) {
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

    @Override
    protected void invokeFinishBundle() throws Exception {
        // TODO: Support batches invoking.
    }

    @Override
    public FlinkFnApi.UserDefinedFunctions getUserDefinedFunctionsProto() {
        FlinkFnApi.UserDefinedFunctions.Builder builder =
                FlinkFnApi.UserDefinedFunctions.newBuilder();
        // add udf proto
        for (PythonFunctionInfo pythonFunctionInfo : scalarFunctions) {
            builder.addUdfs(ProtoUtils.getUserDefinedFunctionProto(pythonFunctionInfo));
        }
        builder.setMetricEnabled(config.get(PYTHON_METRIC_ENABLED));
        builder.setProfileEnabled(config.get(PYTHON_PROFILE_ENABLED));
        return builder.build();
    }
}
