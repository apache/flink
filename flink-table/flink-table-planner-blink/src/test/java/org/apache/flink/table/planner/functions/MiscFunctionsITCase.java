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

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/** Tests for miscellaneous {@link BuiltInFunctionDefinitions}. */
public class MiscFunctionsITCase extends BuiltInFunctionTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.TYPE_OF)
                        .onFieldsWithData(12, "Hello world", false)
                        .testResult(
                                call("TYPEOF", $("f0")),
                                "TYPEOF(f0)",
                                "INT NOT NULL",
                                DataTypes.STRING())
                        .testTableApiError(
                                call("TYPEOF", $("f0"), $("f2")), "Invalid input arguments.")
                        .testSqlError(
                                "TYPEOF(f0, f2)",
                                "SQL validation failed. Invalid function call:\nTYPEOF(INT NOT NULL, BOOLEAN NOT NULL)")
                        .testTableApiResult(
                                call("TYPEOF", $("f1"), true),
                                "CHAR(11) NOT NULL",
                                DataTypes.STRING())
                        .testSqlResult("TYPEOF(NULL)", "NULL", DataTypes.STRING()));
    }
}
