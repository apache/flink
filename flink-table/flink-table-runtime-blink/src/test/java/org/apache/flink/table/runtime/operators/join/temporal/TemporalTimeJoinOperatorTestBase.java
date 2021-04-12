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

package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

/** Base test class for TemporalJoinOperator. */
abstract class TemporalTimeJoinOperatorTestBase {
    protected String funcCode =
            "public class TimeTemporalJoinCondition extends org.apache.flink.api.common.functions.AbstractRichFunction "
                    + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                    + "\n"
                    + "    public TimeTemporalJoinCondition(Object[] reference) {\n"
                    + "    }\n"
                    + "\n"
                    + "    @Override\n"
                    + "    public boolean apply(org.apache.flink.table.data.RowData in1, org.apache.flink.table.data.RowData in2) {\n"
                    + "        return true;\n"
                    + "    }\n"
                    + "}\n";
    protected GeneratedJoinCondition joinCondition =
            new GeneratedJoinCondition("TimeTemporalJoinCondition", funcCode, new Object[0]);
    protected InternalTypeInfo<RowData> rowType =
            InternalTypeInfo.ofFields(
                    new BigIntType(),
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new VarCharType(VarCharType.MAX_LENGTH));
    protected InternalTypeInfo<RowData> outputRowType =
            InternalTypeInfo.ofFields(
                    new BigIntType(),
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new BigIntType(),
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new VarCharType(VarCharType.MAX_LENGTH));
    protected RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(outputRowType.toRowFieldTypes());
    protected int keyIdx = 1;
    protected RowDataKeySelector keySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {keyIdx}, rowType.toRowFieldTypes());
    protected TypeInformation<RowData> keyType = keySelector.getProducedType();
}
