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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/** Tests for logical {@link BuiltInFunctionDefinitions}. */
public class LogicalFunctionsITCase extends BuiltInFunctionTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        List<TestSpec> cases = new ArrayList<>();
        cases.addAll(not());
        return cases;
    }

    private static List<TestSpec> not() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.NOT)
                        .onFieldsWithData(true)
                        .andDataTypes(DataTypes.BOOLEAN().notNull())
                        .testResult($("f0").not(), "NOT f0", false, DataTypes.BOOLEAN().notNull()),
                TestSpec.forFunction(BuiltInFunctionDefinitions.NOT)
                        .onFieldsWithData(false)
                        .andDataTypes(DataTypes.BOOLEAN().notNull())
                        .testResult($("f0").not(), "NOT f0", true, DataTypes.BOOLEAN().notNull()),
                TestSpec.forFunction(BuiltInFunctionDefinitions.NOT)
                        .onFieldsWithData((Boolean) null)
                        .andDataTypes(DataTypes.BOOLEAN())
                        .testResult($("f0").not(), "NOT f0", null, DataTypes.BOOLEAN()));
    }
}
