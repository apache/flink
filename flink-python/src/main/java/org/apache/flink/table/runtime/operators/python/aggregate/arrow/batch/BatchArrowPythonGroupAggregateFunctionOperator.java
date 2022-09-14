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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.types.logical.RowType;

/** The Batch Arrow Python {@link AggregateFunction} Operator for Group Aggregation. */
@Internal
public class BatchArrowPythonGroupAggregateFunctionOperator
        extends AbstractBatchArrowPythonAggregateFunctionOperator {

    private static final long serialVersionUID = 1L;

    public BatchArrowPythonGroupAggregateFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] pandasAggFunctions,
            RowType inputType,
            RowType udfInputType,
            RowType udfOutputType,
            GeneratedProjection inputGeneratedProjection,
            GeneratedProjection groupKeyGeneratedProjection,
            GeneratedProjection groupSetGeneratedProjection) {
        super(
                config,
                pandasAggFunctions,
                inputType,
                udfInputType,
                udfOutputType,
                inputGeneratedProjection,
                groupKeyGeneratedProjection,
                groupSetGeneratedProjection);
    }

    @Override
    protected void invokeCurrentBatch() throws Exception {
        if (currentBatchCount > 0) {
            arrowSerializer.finishCurrentBatch();
            pythonFunctionRunner.process(baos.toByteArray());
            baos.reset();
            elementCount += currentBatchCount;
            checkInvokeFinishBundleByCount();
            currentBatchCount = 0;
            arrowSerializer.resetWriter();
        }
    }

    @Override
    public void bufferInput(RowData input) throws Exception {
        BinaryRowData currentKey = groupKeyProjection.apply(input).copy();
        if (isNewKey(currentKey)) {
            if (lastGroupKey != null) {
                invokeCurrentBatch();
            }
            lastGroupKey = currentKey;
            lastGroupSet = groupSetProjection.apply(input).copy();
            forwardedInputQueue.add(lastGroupSet);
        }
    }

    @Override
    public void processElementInternal(RowData value) {
        arrowSerializer.write(getFunctionInput(value));
        currentBatchCount++;
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public void emitResult(Tuple3<String, byte[], Integer> resultTuple) throws Exception {
        byte[] udafResult = resultTuple.f1;
        int length = resultTuple.f2;
        bais.setBuffer(udafResult, 0, length);
        int rowCount = arrowSerializer.load();
        for (int i = 0; i < rowCount; i++) {
            RowData key = forwardedInputQueue.poll();
            reuseJoinedRow.setRowKind(key.getRowKind());
            RowData result = arrowSerializer.read(i);
            rowDataWrapper.collect(reuseJoinedRow.replace(key, result));
        }
        arrowSerializer.resetReader();
    }
}
