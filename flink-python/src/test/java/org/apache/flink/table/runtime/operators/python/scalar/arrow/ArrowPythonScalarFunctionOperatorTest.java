/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.python.scalar.arrow;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.operators.python.scalar.AbstractPythonScalarFunctionOperator;
import org.apache.flink.table.runtime.operators.python.scalar.PythonScalarFunctionOperatorTestBase;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.runtime.utils.PassThroughPythonScalarFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Queue;

/** Tests for {@link ArrowPythonScalarFunctionOperator}. */
public class ArrowPythonScalarFunctionOperatorTest
        extends PythonScalarFunctionOperatorTestBase<CRow, CRow, Row> {

    public AbstractPythonScalarFunctionOperator<CRow, CRow, Row> getTestOperator(
            Configuration config,
            PythonFunctionInfo[] scalarFunctions,
            RowType inputType,
            RowType outputType,
            int[] udfInputOffsets,
            int[] forwardedFields) {
        return new PassThroughArrowPythonScalarFunctionOperator(
                config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
    }

    public CRow newRow(boolean accumulateMsg, Object... fields) {
        return new CRow(Row.of(fields), accumulateMsg);
    }

    public void assertOutputEquals(
            String message, Collection<Object> expected, Collection<Object> actual) {
        TestHarnessUtil.assertOutputEquals(
                message, (Queue<Object>) expected, (Queue<Object>) actual);
    }

    public StreamTableEnvironment createTableEnvironment(StreamExecutionEnvironment env) {
        return StreamTableEnvironment.create(env);
    }

    @Override
    public TypeSerializer<CRow> getOutputTypeSerializer(RowType dataType) {
        // If set to null, PojoSerializer is used by default which works well here.
        return null;
    }

    private static class PassThroughArrowPythonScalarFunctionOperator
            extends ArrowPythonScalarFunctionOperator {

        PassThroughArrowPythonScalarFunctionOperator(
                Configuration config,
                PythonFunctionInfo[] scalarFunctions,
                RowType inputType,
                RowType outputType,
                int[] udfInputOffsets,
                int[] forwardedFields) {
            super(config, scalarFunctions, inputType, outputType, udfInputOffsets, forwardedFields);
        }

        @Override
        public PythonFunctionRunner createPythonFunctionRunner() throws IOException {
            return new PassThroughPythonScalarFunctionRunner(
                    getRuntimeContext().getTaskName(),
                    PythonTestUtils.createTestEnvironmentManager(),
                    userDefinedFunctionInputType,
                    userDefinedFunctionOutputType,
                    getFunctionUrn(),
                    getUserDefinedFunctionsProto(),
                    getInputOutputCoderUrn(),
                    new HashMap<>(),
                    PythonTestUtils.createMockFlinkMetricContainer());
        }
    }
}
