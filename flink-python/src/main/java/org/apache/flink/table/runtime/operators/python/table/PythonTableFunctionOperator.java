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

package org.apache.flink.table.runtime.operators.python.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.python.AbstractStatelessFunctionOperator;
import org.apache.flink.table.runtime.operators.python.utils.StreamRecordRowDataWrappingCollector;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.util.ProtoUtils.createFlattenRowTypeCoderInfoDescriptorProto;
import static org.apache.flink.python.util.ProtoUtils.createRowTypeCoderInfoDescriptorProto;

/** The Python {@link TableFunction} operator. */
@Internal
public class PythonTableFunctionOperator
        extends AbstractStatelessFunctionOperator<RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final String TABLE_FUNCTION_URN = "flink:transform:table_function:v1";

    /** The Python {@link TableFunction} to be executed. */
    private final PythonFunctionInfo tableFunction;

    /** The correlate join type. */
    private final FlinkJoinType joinType;

    private final GeneratedProjection udtfInputGeneratedProjection;

    /** The collector used to collect records. */
    private transient StreamRecordRowDataWrappingCollector rowDataWrapper;

    /** The JoinedRowData reused holding the execution result. */
    private transient JoinedRowData reuseJoinedRow;

    /** The Projection which projects the udtf input fields from the input row. */
    private transient Projection<RowData, BinaryRowData> udtfInputProjection;

    /** The TypeSerializer for udtf execution results. */
    private transient TypeSerializer<RowData> udtfOutputTypeSerializer;

    /** The TypeSerializer for udtf input elements. */
    private transient TypeSerializer<RowData> udtfInputTypeSerializer;

    /** The type serializer for the forwarded fields. */
    private transient RowDataSerializer forwardedInputSerializer;

    /** The current input element which has not been received all python udtf results. */
    private transient RowData input;

    /** Whether the current input element has joined parts of python udtf results. */
    private transient boolean hasJoined;

    /** Whether the current received data is the finished result of the current input element. */
    private transient boolean isFinishResult;

    public PythonTableFunctionOperator(
            Configuration config,
            PythonFunctionInfo tableFunction,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            FlinkJoinType joinType,
            GeneratedProjection udtfInputGeneratedProjection) {
        super(config, inputType, udfInputType, udfOutputType);
        this.tableFunction = Preconditions.checkNotNull(tableFunction);
        Preconditions.checkArgument(
                joinType == FlinkJoinType.INNER || joinType == FlinkJoinType.LEFT,
                "The join type should be inner join or left join");
        this.joinType = joinType;
        this.udtfInputGeneratedProjection =
                Preconditions.checkNotNull(udtfInputGeneratedProjection);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open() throws Exception {
        super.open();
        rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
        reuseJoinedRow = new JoinedRowData();

        udtfInputProjection =
                udtfInputGeneratedProjection.newInstance(
                        Thread.currentThread().getContextClassLoader());
        forwardedInputSerializer = new RowDataSerializer(inputType);
        udtfInputTypeSerializer = PythonTypeUtils.toInternalSerializer(udfInputType);
        udtfOutputTypeSerializer = PythonTypeUtils.toInternalSerializer(udfOutputType);
        input = null;
        hasJoined = false;
        isFinishResult = true;
    }

    @Override
    public PythonEnv getPythonEnv() {
        return tableFunction.getPythonFunction().getPythonEnv();
    }

    @Override
    public String getFunctionUrn() {
        return TABLE_FUNCTION_URN;
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createInputCoderInfoDescriptor(RowType runnerInputType) {
        if (tableFunction.getPythonFunction().takesRowAsInput()) {
            // need the field names in case of row-based operations
            return createRowTypeCoderInfoDescriptorProto(
                    runnerInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, true);
        } else {
            return createFlattenRowTypeCoderInfoDescriptorProto(
                    runnerInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, true);
        }
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createOutputCoderInfoDescriptor(RowType runnerOutType) {
        return createFlattenRowTypeCoderInfoDescriptorProto(
                runnerOutType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, true);
    }

    @Override
    public FlinkFnApi.UserDefinedFunctions createUserDefinedFunctionsProto() {
        return ProtoUtils.createUserDefinedFunctionsProto(
                getRuntimeContext(),
                new PythonFunctionInfo[] {tableFunction},
                config.get(PYTHON_METRIC_ENABLED),
                config.get(PYTHON_PROFILE_ENABLED));
    }

    @Override
    public void bufferInput(RowData input) {
        // always copy the input RowData
        RowData forwardedFields = forwardedInputSerializer.copy(input);
        forwardedFields.setRowKind(input.getRowKind());
        forwardedInputQueue.add(forwardedFields);
    }

    @Override
    public RowData getFunctionInput(RowData element) {
        return udtfInputProjection.apply(element);
    }

    @Override
    public void processElementInternal(RowData value) throws Exception {
        udtfInputTypeSerializer.serialize(getFunctionInput(value), baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public void emitResult(Tuple3<String, byte[], Integer> resultTuple) throws Exception {
        byte[] rawUdtfResult;
        int length;
        if (isFinishResult) {
            input = forwardedInputQueue.poll();
            hasJoined = false;
        }
        do {
            rawUdtfResult = resultTuple.f1;
            length = resultTuple.f2;
            isFinishResult = isFinishResult(rawUdtfResult, length);
            if (!isFinishResult) {
                reuseJoinedRow.setRowKind(input.getRowKind());
                bais.setBuffer(rawUdtfResult, 0, rawUdtfResult.length);
                RowData udtfResult = udtfOutputTypeSerializer.deserialize(baisWrapper);
                rowDataWrapper.collect(reuseJoinedRow.replace(input, udtfResult));
                resultTuple = pythonFunctionRunner.pollResult();
                hasJoined = true;
            } else if (joinType == FlinkJoinType.LEFT && !hasJoined) {
                GenericRowData udtfResult = new GenericRowData(udfOutputType.getFieldCount());
                for (int i = 0; i < udtfResult.getArity(); i++) {
                    udtfResult.setField(i, null);
                }
                rowDataWrapper.collect(reuseJoinedRow.replace(input, udtfResult));
            }
        } while (!isFinishResult && resultTuple != null);
    }

    /** The received udtf execution result is a finish message when it is a byte with value 0x00. */
    private boolean isFinishResult(byte[] rawUdtfResult, int length) {
        return length == 1 && rawUdtfResult[0] == 0x00;
    }
}
