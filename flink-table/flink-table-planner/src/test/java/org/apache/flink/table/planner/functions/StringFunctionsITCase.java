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

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/** Test String functions correct behaviour. */
class StringFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.REGEXP_EXTRACT, "Check return type")
                        .onFieldsWithData("22", "ABC")
                        .testResult(
                                call("regexpExtract", $("f0"), "[A-Z]+"),
                                "REGEXP_EXTRACT(f0,'[A-Z]+')",
                                null,
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("regexpExtract", $("f1"), "[A-Z]+"),
                                "REGEXP_EXTRACT(f1, '[A-Z]+')",
                                "ABC",
                                DataTypes.STRING().nullable()));
    }
}
