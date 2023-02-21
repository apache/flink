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

import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.coalesce;

/** Test {@link BuiltInFunctionDefinitions#COALESCE} and its return type. */
class CoalesceFunctionITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.COALESCE)
                        .onFieldsWithData(null, null, 1)
                        .andDataTypes(BIGINT().nullable(), INT().nullable(), INT().notNull())
                        .testResult(
                                coalesce($("f0"), $("f1")),
                                "COALESCE(f0, f1)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                coalesce($("f0"), $("f2")),
                                "COALESCE(f0, f2)",
                                1L,
                                BIGINT().notNull())
                        .testResult(
                                coalesce($("f1"), $("f2")), "COALESCE(f1, f2)", 1, INT().notNull())
                        .testResult(
                                coalesce($("f0"), 1),
                                "COALESCE(f0, 1)",
                                1L,
                                // In this case, the return type is not null because we have a
                                // constant in the function invocation
                                BIGINT().notNull()));
    }
}
