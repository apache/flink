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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Collection;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.binaryrow;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;

/** Base class for Arrow Python aggregate function operator tests. */
public abstract class ArrowPythonAggregateFunctionOperatorTestBase {

    private RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(getOutputLogicalType());

    protected RowData newRow(boolean accumulateMsg, Object... fields) {
        if (accumulateMsg) {
            return row(fields);
        } else {
            RowData row = row(fields);
            row.setRowKind(RowKind.DELETE);
            return row;
        }
    }

    protected RowData newBinaryRow(boolean accumulateMsg, Object... fields) {
        if (accumulateMsg) {
            return binaryrow(fields);
        } else {
            RowData row = binaryrow(fields);
            row.setRowKind(RowKind.DELETE);
            return row;
        }
    }

    protected void assertOutputEquals(
            String message, Collection<Object> expected, Collection<Object> actual) {
        assertor.assertOutputEquals(message, expected, actual);
    }

    public abstract OneInputStreamOperatorTestHarness<RowData, RowData> getTestHarness(
            Configuration config) throws Exception;

    public abstract LogicalType[] getOutputLogicalType();

    public abstract RowType getInputType();

    public abstract RowType getOutputType();

    public abstract AbstractArrowPythonAggregateFunctionOperator getTestOperator(
            Configuration config,
            PythonFunctionInfo[] pandasAggregateFunctions,
            RowType inputType,
            RowType outputType,
            int[] groupingSet,
            int[] udafInputOffsets);
}
