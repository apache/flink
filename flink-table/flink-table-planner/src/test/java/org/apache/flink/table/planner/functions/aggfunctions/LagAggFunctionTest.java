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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.data.StringData.fromString;

/** Test for {@link LagAggFunction}. */
public class LagAggFunctionTest
        extends AggFunctionTestBase<StringData, LagAggFunction.LagAcc<StringData>> {

    @Override
    protected List<List<StringData>> getInputValueSets() {
        return Arrays.asList(
                Collections.singletonList(fromString("1")),
                Arrays.asList(fromString("1"), null),
                Arrays.asList(null, null),
                Arrays.asList(null, fromString("10")));
    }

    @Override
    protected List<StringData> getExpectedResults() {
        return Arrays.asList(null, fromString("1"), null, null);
    }

    @Override
    protected AggregateFunction<StringData, LagAggFunction.LagAcc<StringData>> getAggregator() {
        return new LagAggFunction<>(
                new LogicalType[] {new VarCharType(), new IntType(), new CharType()});
    }

    @Override
    protected Class<?> getAccClass() {
        return LagAggFunction.LagAcc.class;
    }
}
