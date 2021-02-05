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

package org.apache.flink.table.runtime.functions.python.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.arrow.serializers.ArrowSerializer;
import org.apache.flink.table.runtime.arrow.serializers.RowArrowSerializer;
import org.apache.flink.table.runtime.functions.python.AbstractPythonScalarFunctionFlatMap;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * The {@link RichFlatMapFunction} used to invoke Arrow Python {@link ScalarFunction} functions for
 * the old planner.
 */
@Internal
public final class ArrowPythonScalarFunctionFlatMap extends AbstractPythonScalarFunctionFlatMap {

    private static final long serialVersionUID = 1L;

    private static final String SCHEMA_ARROW_CODER_URN = "flink:coder:schema:arrow:v1";

    /** The current number of elements to be included in an arrow batch. */
    private transient int currentBatchCount;

    /** Max number of elements to include in an arrow batch. */
    private final int maxArrowBatchSize;

    private transient ArrowSerializer<Row> arrowSerializer;

    public ArrowPythonScalarFunctionFlatMap(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType outputType,
            int[] udfInputOffsets,
            int[] forwardedFields) {
        super(config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
        maxArrowBatchSize = getPythonConfig().getMaxArrowBatchSize();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        arrowSerializer =
                new RowArrowSerializer(userDefinedFunctionInputType, userDefinedFunctionOutputType);
        arrowSerializer.open(bais, baos);
        currentBatchCount = 0;
    }

    @Override
    public void close() throws Exception {
        invokeCurrentBatch();
        try {
            super.close();
        } finally {
            arrowSerializer.close();
        }
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws IOException {
        byte[] udfResult = resultTuple.f0;
        int length = resultTuple.f1;
        bais.setBuffer(udfResult, 0, length);
        int rowCount = arrowSerializer.load();
        for (int i = 0; i < rowCount; i++) {
            resultCollector.collect(Row.join(forwardedInputQueue.poll(), arrowSerializer.read(i)));
        }
        arrowSerializer.resetReader();
    }

    @Override
    public String getInputOutputCoderUrn() {
        return SCHEMA_ARROW_CODER_URN;
    }

    @Override
    public void processElementInternal(Row value) throws Exception {
        arrowSerializer.write(getFunctionInput(value));
        currentBatchCount++;
        if (currentBatchCount >= maxArrowBatchSize) {
            invokeCurrentBatch();
        }
    }

    @Override
    protected void invokeFinishBundle() throws Exception {
        invokeCurrentBatch();
        super.invokeFinishBundle();
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
