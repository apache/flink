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
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/** IT tests for {@link BuiltInFunctionDefinitions#IF}. */
class IfThenElseFunctionITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.IF)
                        .onFieldsWithData(2)
                        .andDataTypes(DataTypes.INT())
                        .testResult(
                                Expressions.ifThenElse(
                                        $("f0").isGreater(lit(0)), lit("GREATER"), lit("SMALLER")),
                                "CASE WHEN f0 > 0 THEN 'GREATER' ELSE 'SMALLER' END",
                                "GREATER",
                                DataTypes.CHAR(7).notNull())
                        .testResult(
                                Expressions.ifThenElse(
                                        $("f0").isGreater(lit(0)),
                                        lit("GREATER"),
                                        Expressions.ifThenElse(
                                                $("f0").isEqual(0), lit("EQUAL"), lit("SMALLER"))),
                                "CASE WHEN f0 > 0 THEN 'GREATER' ELSE CASE WHEN f0 = 0 THEN 'EQUAL' ELSE 'SMALLER' END END",
                                "GREATER",
                                DataTypes.VARCHAR(7).notNull()));
    }
}
