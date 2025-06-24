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

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.types.RowKind.INSERT;

/** Tests for built-in ARRAY_AGG aggregation functions. */
class MiscAggFunctionITCase extends BuiltInAggregateFunctionTestBase {

    @Override
    Stream<TestSpec> getTestCaseSpecs() {
        return Stream.of(
                TestSpec.forExpression("SINGLE_VALUE")
                        .withSource(
                                ROW(STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", 1),
                                        Row.ofKind(INSERT, "A", 2),
                                        Row.ofKind(INSERT, "B", 2)))
                        .testSqlRuntimeError(
                                source ->
                                        "SELECT f0, SINGLE_VALUE(f1) FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(STRING(), INT()),
                                TableRuntimeException.class,
                                "SingleValueAggFunction received more than one element."));
    }
}
