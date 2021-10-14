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

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

/** Base class of tests for all kinds of row-time DeduplicateFunction. */
abstract class RowTimeDeduplicateFunctionTestBase {

    protected final long miniBatchSize = 4L;
    protected Time minTtlTime = Time.milliseconds(10);
    protected InternalTypeInfo inputRowType =
            InternalTypeInfo.ofFields(
                    new VarCharType(VarCharType.MAX_LENGTH), new IntType(), new BigIntType());
    protected TypeSerializer<RowData> serializer = inputRowType.toSerializer();
    protected int rowTimeIndex = 2;
    protected int rowKeyIndex = 0;
    protected RowDataKeySelector rowKeySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {rowKeyIndex}, inputRowType.toRowFieldTypes());
    protected RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(
                    inputRowType.toRowFieldTypes(),
                    new GenericRowRecordSortComparator(
                            rowKeyIndex, inputRowType.toRowFieldTypes()[rowKeyIndex]));

    protected OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            KeyedProcessOperator<RowData, RowData, RowData> operator) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, rowKeySelector, rowKeySelector.getProducedType());
    }

    protected OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            KeyedMapBundleOperator<RowData, RowData, RowData, RowData> operator) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, rowKeySelector, rowKeySelector.getProducedType());
    }
}
