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

package org.apache.flink.table.runtime.operators.join.interval;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;

/** Base Test for all subclass of {@link TimeIntervalJoin}. */
abstract class TimeIntervalStreamJoinTestBase {
    InternalTypeInfo<RowData> rowType =
            InternalTypeInfo.ofFields(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));

    private InternalTypeInfo<RowData> outputRowType =
            InternalTypeInfo.ofFields(
                    new BigIntType(),
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new BigIntType(),
                    new VarCharType(VarCharType.MAX_LENGTH));
    RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(outputRowType.toRowFieldTypes());

    protected String funcCode =
            "public class TestIntervalJoinCondition extends org.apache.flink.api.common.functions.AbstractRichFunction "
                    + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                    + "\n"
                    + "    public TestIntervalJoinCondition(Object[] reference) {\n"
                    + "    }\n"
                    + "\n"
                    + "    @Override\n"
                    + "    public boolean apply(org.apache.flink.table.data.RowData in1, org.apache.flink.table.data.RowData in2) {\n"
                    + "        return true;\n"
                    + "    }\n"
                    + "}\n";
    protected IntervalJoinFunction joinFunction =
            new IntervalJoinFunction(
                    new GeneratedJoinCondition(
                            "TestIntervalJoinCondition", funcCode, new Object[0]),
                    outputRowType,
                    new boolean[] {true});
}
