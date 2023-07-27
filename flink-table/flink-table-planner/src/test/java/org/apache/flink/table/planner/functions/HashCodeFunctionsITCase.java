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

import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.Expressions.hashCodeGen;

/** Tests for built-in HASHCODE functions. */
public class HashCodeFunctionsITCase extends BuiltInFunctionTestBase {
    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        final List<TestSetSpec> testCases = new ArrayList<>();
        testCases.addAll(hashCodeSpec());

        return testCases.stream();
    }

    private static List<TestSetSpec> hashCodeSpec() {
        return Arrays.asList(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.HASHCODE)
                        .onFieldsWithData(
                                "V",
                                true,
                                1,
                                Row.of("R1", Instant.parse("1990-06-02T13:37:42.001Z")))
                        .andDataTypes(
                                STRING().notNull(),
                                BOOLEAN().notNull(),
                                INT().notNull(),
                                ROW(STRING(), TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)).notNull())
                        .testResult(
                                hashCodeGen(Expressions.$("f3")),
                                "HASHCODE(f3)",
                                58542757,
                                INT().notNull()));
    }
}
