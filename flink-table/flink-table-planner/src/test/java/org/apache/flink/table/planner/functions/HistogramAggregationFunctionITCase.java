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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.util.CollectionUtil.entry;

/** Tests for built-in JSON aggregation functions. */
class HistogramAggregationFunctionITCase extends BuiltInAggregateFunctionTestBase {

    @Override
    public Stream<TestSpec> getTestCaseSpecs() {
        return Stream.of(
                TestSpec.forFunction(BuiltInFunctionDefinitions.HISTOGRAM)
                        .withDescription("Basic Aggregation")
                        .withSource(
                                ROW(STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", 1),
                                        Row.ofKind(INSERT, "B", null),
                                        Row.ofKind(INSERT, "C", 3)))
                        .testResult(
                                source -> "SELECT HISTOGRAM(f1) FROM " + source,
                                source -> source.select(($("f1").histogram())),
                                ROW(MULTISET(INT()).notNull()),
                                ROW(MULTISET(INT())),
                                Collections.singletonList(
                                        Row.of(CollectionUtil.map(entry(1, 1), entry(3, 1)))))
                        .testResult(
                                source -> "SELECT HISTOGRAM(f0) FROM " + source,
                                source -> source.select(($("f0").histogram())),
                                ROW(MULTISET(STRING()).notNull()),
                                ROW(MULTISET(STRING())),
                                Collections.singletonList(
                                        Row.of(
                                                CollectionUtil.map(
                                                        entry("A", 1),
                                                        entry("B", 1),
                                                        entry("C", 1))))));
    }
}
