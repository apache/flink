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
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/** Test {@link BuiltInFunctionDefinitions#CONV} and its return type. */
class ConvFunctionITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.CONV)
                        .onFieldsWithData(null, 44, 43, 100, -1, "facebook")
                        .andDataTypes(
                                BIGINT().nullable(),
                                TINYINT().notNull(),
                                SMALLINT().notNull(),
                                BIGINT().notNull(),
                                BIGINT().notNull(),
                                STRING().notNull())
                        // test for all LogicalTypeFamily.INTEGER_NUMERIC type.
                        .testResult(
                                resultSpec(
                                        call("conv", $("f1"), 10, 16),
                                        "conv(f1, 10, 16)",
                                        "2C",
                                        STRING().nullable()))
                        .testResult(
                                resultSpec(
                                        call("conv", $("f2"), 10, 16),
                                        "conv(f2, 10, 16)",
                                        "2B",
                                        STRING().nullable()))
                        .testResult(
                                resultSpec(
                                        call("conv", $("f3"), 10, -8),
                                        "conv(f3, 10, -8)",
                                        "144",
                                        STRING().nullable()))
                        .testResult(
                                resultSpec(
                                        call("conv", $("f3"), 10, -8),
                                        "conv(f3, 10, -8)",
                                        "144",
                                        STRING().nullable()))
                        .testResult(
                                resultSpec(
                                        call("conv", $("f4"), 10, 16),
                                        "conv(f4, 10, 16)",
                                        "FFFFFFFFFFFFFFFF",
                                        STRING().nullable()))
                        .testResult(
                                resultSpec(
                                        call("conv", $("f4"), 10, -16),
                                        "conv(f4, 10, -16)",
                                        "-1",
                                        STRING().nullable()))
                        .testResult(
                                resultSpec(
                                        call("conv", $("f5"), 36, 16),
                                        "conv(f5, 36, 16)",
                                        "116ED2B2FB4",
                                        STRING().nullable()))
                        .testResult(
                                resultSpec(
                                        call("conv", 1111, 2, 10),
                                        "conv(1111, 2, 10)",
                                        "15",
                                        STRING().nullable()))

                        // 2^64 - 1
                        .testResult(
                                resultSpec(
                                        call("conv", "18446744073709551615", 10, 10),
                                        "conv('18446744073709551615', 10, 10)",
                                        "18446744073709551615",
                                        STRING().nullable()))
                        .testResult(
                                resultSpec(
                                        call("conv", "18446744073709551615", 10, -10),
                                        "conv('18446744073709551615', 10, -10)",
                                        "-1",
                                        STRING().nullable()))
                        // overflow, return maximum (2^64 - 1), which is consistent with
                        // Mysql.
                        .testResult(
                                resultSpec(
                                        call("conv", "38446744073709551615", 10, 10),
                                        "conv('38446744073709551615', 10, 10)",
                                        "18446744073709551615",
                                        STRING().nullable()))

                        // args is null
                        .testResult(
                                resultSpec(
                                        call("conv", $("f0"), 10, 2),
                                        "conv(f0, 10, 2)",
                                        null,
                                        STRING().nullable()))
                        .testResult(
                                resultSpec(
                                        call("conv", $("f0"), null, 2),
                                        "conv(f0, null, 2)",
                                        null,
                                        STRING().nullable()))

                        // the input is not a decimal number.
                        .testResult(
                                resultSpec(
                                        call("conv", "This is a test String.", 10, 16),
                                        "conv('This is a test String.', 10, 16)",
                                        null,
                                        STRING().nullable()))

                        // fromBase cannot be negative.
                        .testResult(
                                resultSpec(
                                        call("conv", $("f2"), -10, 16),
                                        "conv(f2, -10, 16)",
                                        null,
                                        STRING().nullable())));
    }
}
