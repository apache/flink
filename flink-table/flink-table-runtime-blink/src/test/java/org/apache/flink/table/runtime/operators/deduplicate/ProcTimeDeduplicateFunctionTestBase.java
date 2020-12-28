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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

/** Base class of tests for all kinds of processing-time DeduplicateFunction. */
abstract class ProcTimeDeduplicateFunctionTestBase {

    Time minTime = Time.milliseconds(10);
    InternalTypeInfo<RowData> inputRowType =
            InternalTypeInfo.ofFields(
                    new VarCharType(VarCharType.MAX_LENGTH), new BigIntType(), new IntType());

    int rowKeyIdx = 1;
    BinaryRowDataKeySelector rowKeySelector =
            new BinaryRowDataKeySelector(new int[] {rowKeyIdx}, inputRowType.toRowFieldTypes());

    RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(
                    inputRowType.toRowFieldTypes(),
                    new GenericRowRecordSortComparator(
                            rowKeyIdx, inputRowType.toRowFieldTypes()[rowKeyIdx]));
}
