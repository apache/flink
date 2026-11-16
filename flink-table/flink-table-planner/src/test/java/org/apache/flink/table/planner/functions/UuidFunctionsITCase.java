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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.uuidV4;
import static org.apache.flink.table.api.Expressions.uuidV7;

/**
 * Test for {@link BuiltInFunctionDefinitions#UUID_V4} and {@link
 * BuiltInFunctionDefinitions#UUID_V7} and their return type.
 */
public class UuidFunctionsITCase extends BuiltInFunctionTestBase {

    private static final String MIN = "UUID '00000000-0000-0000-0000-000000000000'";
    private static final String MID = "UUID '8ba7b810-9dad-11d1-80b4-00c04fd430c8'";
    private static final String MAX = "UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff'";

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                // UUID_V4()
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.UUID_V4)
                        .testSqlResult("UUID_V4()", DataTypes.UUID().notNull())
                        .testTableApiResult(uuidV4(), DataTypes.UUID().notNull()),
                // UUID_V4() produces a version 4 UUID, i.e. the canonical string form has '4' as
                // the first character of the third group.
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.UUID_V4)
                        .testSqlResult(
                                "CHAR_LENGTH(SPLIT_INDEX(CAST(UUID_V4() AS STRING), '-', 2))",
                                4,
                                DataTypes.INT())
                        .testSqlResult(
                                "SUBSTR(SPLIT_INDEX(CAST(UUID_V4() AS STRING), '-', 2), 1, 1)",
                                "4",
                                DataTypes.STRING()),
                // UUID_V7()
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.UUID_V7)
                        .testSqlResult("UUID_V7()", DataTypes.UUID().notNull())
                        .testTableApiResult(uuidV7(), DataTypes.UUID().notNull()),
                // UUID_V7() produces a version 7 UUID, i.e. the canonical string form has '7' as
                // the first character of the third group.
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.UUID_V7)
                        .testSqlResult(
                                "CHAR_LENGTH(SPLIT_INDEX(CAST(UUID_V7() AS STRING), '-', 2))",
                                4,
                                DataTypes.INT())
                        .testSqlResult(
                                "SUBSTR(SPLIT_INDEX(CAST(UUID_V7() AS STRING), '-', 2), 1, 1)",
                                "7",
                                DataTypes.STRING()),
                // Range predicates on UUID literals use unsigned ordering
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BETWEEN, "UUID literals")
                        .testSqlResult(
                                MID + " BETWEEN " + MIN + " AND " + MAX,
                                true,
                                DataTypes.BOOLEAN().notNull())
                        .testSqlResult(
                                MIN + " BETWEEN " + MIN + " AND " + MAX,
                                true,
                                DataTypes.BOOLEAN().notNull())
                        .testSqlResult(
                                MAX + " BETWEEN " + MIN + " AND " + MAX,
                                true,
                                DataTypes.BOOLEAN().notNull())
                        .testSqlResult(
                                MID + " NOT BETWEEN " + MIN + " AND " + MAX,
                                false,
                                DataTypes.BOOLEAN().notNull())
                        .testSqlResult(MID + " > " + MIN, true, DataTypes.BOOLEAN().notNull())
                        .testSqlResult(MAX + " < " + MIN, false, DataTypes.BOOLEAN().notNull())
                        .testSqlResult(
                                MID + " IN (" + MIN + ", " + MAX + ")",
                                false,
                                DataTypes.BOOLEAN().notNull())
                        .testSqlResult(
                                MAX + " IN (" + MIN + ", " + MAX + ")",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        .testSqlResult(
                                MID + " NOT IN (" + MIN + ", " + MAX + ")",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        .testSqlResult(
                                MAX + " IS DISTINCT FROM " + MIN,
                                true,
                                DataTypes.BOOLEAN().notNull()));
    }
}
