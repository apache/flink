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
import org.apache.flink.table.runtime.functions.aggregate.ModeAggFunction;
import org.apache.flink.table.types.logical.VarCharType;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.data.StringData.fromString;

/** Test case for built-in MODE with retraction aggregate function. */
final class ModeAggFunctionTest
        extends AggFunctionTestBase<StringData, StringData, ModeAggFunction.ModeAcc<StringData>> {

    @Override
    protected List<List<StringData>> getInputValueSets() {
        return Arrays.asList(
                Arrays.asList(fromString("1"), fromString("1"), fromString("3")),
                Arrays.asList(fromString("4"), null, fromString("4")),
                Arrays.asList(null, null),
                Arrays.asList(
                        null,
                        fromString("2"),
                        fromString("1"),
                        fromString("2"),
                        fromString("2"),
                        fromString("1")),
                Collections.singletonList(null));
    }

    @Override
    protected List<StringData> getExpectedResults() {
        return Arrays.asList(fromString("1"), fromString("4"), null, fromString("2"), null);
    }

    @Override
    protected ModeAggFunction<StringData> getAggregator() {
        return new ModeAggFunction<>(new VarCharType());
    }

    @Override
    protected Class<?> getAccClass() {
        return ModeAggFunction.ModeAcc.class;
    }

    @Override
    protected Method getAccumulateFunc() throws NoSuchMethodException {
        return getAggregator().getClass().getMethod("accumulate", getAccClass(), Object.class);
    }

    @Override
    protected Method getRetractFunc() throws NoSuchMethodException {
        return getAggregator().getClass().getMethod("retract", getAccClass(), Object.class);
    }
}
