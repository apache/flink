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

package org.apache.flink.table.runtime.operators.python.scalar.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.arrow.serializers.ArrowSerializer;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.operators.python.scalar.AbstractPythonScalarFunctionOperator;
import org.apache.flink.table.types.logical.RowType;

import static org.apache.flink.python.PythonOptions.MAX_ARROW_BATCH_SIZE;
import static org.apache.flink.python.util.ProtoUtils.createArrowTypeCoderInfoDescriptorProto;

/** Arrow Python {@link ScalarFunction} operator. */
@Internal
public class ArrowPythonScalarFunctionOperator extends AbstractPythonScalarFunctionOperator {

    private static final long serialVersionUID = 1L;

    /** The current number of elements to be included in an arrow batch. */
    private transient int currentBatchCount;

    /** Max number of elements to include in an arrow batch. */
    private transient int maxArrowBatchSize;

    private transient ArrowSerializer arrowSerializer;

    public ArrowPythonScalarFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            GeneratedProjection udfInputGeneratedProjection,
            GeneratedProjection forwardedFieldGeneratedProjection) {
        super(
                config,
                scalarFunctions,
                inputType,
                udfInputType,
                udfOutputType,
                udfInputGeneratedProjection,
                forwardedFieldGeneratedProjection);
    }

    @Override
    public void open() throws Exception {
        super.open();
        maxArrowBatchSize = Math.min(config.get(MAX_ARROW_BATCH_SIZE), maxBundleSize);
        arrowSerializer = new ArrowSerializer(udfInputType, udfOutputType);
        arrowSerializer.open(bais, baos);
        currentBatchCount = 0;
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createInputCoderInfoDescriptor(RowType runnerInputType) {
        return createArrowTypeCoderInfoDescriptorProto(
                runnerInputType, FlinkFnApi.CoderInfoDescriptor.Mode.MULTIPLE, false);
    }

    @Override
    public FlinkFnApi.CoderInfoDescriptor createOutputCoderInfoDescriptor(
            RowType runnerOutputType) {
        return createArrowTypeCoderInfoDescriptorProto(
                runnerOutputType, FlinkFnApi.CoderInfoDescriptor.Mode.SINGLE, false);
    }

    @Override
    protected void invokeFinishBundle() throws Exception {
        invokeCurrentBatch();
        super.invokeFinishBundle();
    }

    @Override
    public void endInput() throws Exception {
        invokeCurrentBatch();
        super.endInput();
    }

    @Override
    public void finish() throws Exception {
        invokeCurrentBatch();
        super.finish();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (arrowSerializer != null) {
            arrowSerializer.close();
            arrowSerializer = null;
        }
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public void emitResult(Tuple3<String, byte[], Integer> resultTuple) throws Exception {
        byte[] udfResult = resultTuple.f1;
        int length = resultTuple.f2;
        bais.setBuffer(udfResult, 0, length);
        int rowCount = arrowSerializer.load();
        for (int i = 0; i < rowCount; i++) {
            RowData input = forwardedInputQueue.poll();
            reuseJoinedRow.setRowKind(input.getRowKind());
            rowDataWrapper.collect(reuseJoinedRow.replace(input, arrowSerializer.read(i)));
        }
        arrowSerializer.resetReader();
    }

    @Override
    public void processElementInternal(RowData value) throws Exception {
        arrowSerializer.write(getFunctionInput(value));
        currentBatchCount++;
        if (currentBatchCount >= maxArrowBatchSize) {
            invokeCurrentBatch();
        }
    }

    private void invokeCurrentBatch() throws Exception {
        if (currentBatchCount > 0) {
            arrowSerializer.finishCurrentBatch();
            currentBatchCount = 0;
            pythonFunctionRunner.process(baos.toByteArray());
            checkInvokeFinishBundleByCount();
            baos.reset();
            arrowSerializer.resetWriter();
        }
    }
}
