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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

/** The Python {@link ScalarFunction} operator for the legacy planner. */
@Internal
public class PythonScalarFunctionOperator extends AbstractRowPythonScalarFunctionOperator {

    private static final long serialVersionUID = 1L;

    /** The TypeSerializer for udf execution results. */
    private transient TypeSerializer<Row> udfOutputTypeSerializer;

    /** The TypeSerializer for udf input elements. */
    private transient TypeSerializer<Row> udfInputTypeSerializer;

    public PythonScalarFunctionOperator(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType outputType,
            int[] udfInputOffsets,
            int[] forwardedFields) {
        super(config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open() throws Exception {
        super.open();
        udfInputTypeSerializer =
                PythonTypeUtils.toFlinkTypeSerializer(userDefinedFunctionInputType);
        udfOutputTypeSerializer =
                PythonTypeUtils.toFlinkTypeSerializer(userDefinedFunctionOutputType);
    }

    @Override
    public void processElementInternal(CRow value) throws Exception {
        udfInputTypeSerializer.serialize(getFunctionInput(value), baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] rawUdfResult = resultTuple.f0;
        int length = resultTuple.f1;
        CRow input = forwardedInputQueue.poll();
        cRowWrapper.setChange(input.change());
        bais.setBuffer(rawUdfResult, 0, length);
        Row udfResult = udfOutputTypeSerializer.deserialize(baisWrapper);
        cRowWrapper.collect(Row.join(input.row(), udfResult));
    }
}
