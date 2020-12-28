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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.arrow.serializers.ArrowSerializer;
import org.apache.flink.table.runtime.arrow.serializers.RowDataArrowSerializer;
import org.apache.flink.table.runtime.operators.python.scalar.AbstractRowDataPythonScalarFunctionOperator;
import org.apache.flink.table.types.logical.RowType;

/** Arrow Python {@link ScalarFunction} operator for the blink planner. */
@Internal
public class RowDataArrowPythonScalarFunctionOperator
        extends AbstractRowDataPythonScalarFunctionOperator {

    private static final long serialVersionUID = 1L;

    private static final String SCHEMA_ARROW_CODER_URN = "flink:coder:schema:arrow:v1";

    /** The current number of elements to be included in an arrow batch. */
    private transient int currentBatchCount;

    /** Max number of elements to include in an arrow batch. */
    private transient int maxArrowBatchSize;

    private transient ArrowSerializer<RowData> arrowSerializer;

    public RowDataArrowPythonScalarFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType outputType,
            int[] udfInputOffsets,
            int[] forwardedFields) {
        super(config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
    }

    @Override
    public void open() throws Exception {
        super.open();
        maxArrowBatchSize = Math.min(getPythonConfig().getMaxArrowBatchSize(), maxBundleSize);
        arrowSerializer =
                new RowDataArrowSerializer(
                        userDefinedFunctionInputType, userDefinedFunctionOutputType);
        arrowSerializer.open(bais, baos);
        currentBatchCount = 0;
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
    public void dispose() throws Exception {
        super.dispose();
        arrowSerializer.close();
    }

    @Override
    public void close() throws Exception {
        invokeCurrentBatch();
        super.close();
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] udfResult = resultTuple.f0;
        int length = resultTuple.f1;
        bais.setBuffer(udfResult, 0, length);
        int rowCount = arrowSerializer.load();
        for (int i = 0; i < rowCount; i++) {
            RowData input = forwardedInputQueue.poll();
            reuseJoinedRow.setRowKind(input.getRowKind());
            rowDataWrapper.collect(reuseJoinedRow.replace(input, arrowSerializer.read(i)));
        }
    }

    @Override
    public String getInputOutputCoderUrn() {
        return SCHEMA_ARROW_CODER_URN;
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
        }
    }
}
