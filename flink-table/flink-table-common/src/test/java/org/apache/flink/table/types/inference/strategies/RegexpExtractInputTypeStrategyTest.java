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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;

import java.util.stream.Stream;

import static org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies.REGEXP_EXTRACT;

/** Tests for {@link RegexpExtractInputTypeStrategy}. */
class RegexpExtractInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    @Override
    protected Stream<TestSpec> testData() {
        return Stream.of(
                // Non-literal regex skips the plan-time compile check and is deferred to runtime.
                TestSpec.forStrategy("Non-literal regex defers compile to runtime", REGEXP_EXTRACT)
                        .calledWithArgumentTypes(DataTypes.STRING(), DataTypes.STRING())
                        .expectArgumentTypes(DataTypes.STRING(), DataTypes.STRING()),

                // Valid literal regex compiles cleanly at plan time.
                TestSpec.forStrategy("Valid literal regex compiles", REGEXP_EXTRACT)
                        .calledWithArgumentTypes(
                                DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT())
                        .calledWithLiteralAt(1, "foo(.*?)bar")
                        .expectArgumentTypes(
                                DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT()),

                // Null literal regex short-circuits the plan-time check; runtime returns null.
                TestSpec.forStrategy("Null regex literal is deferred", REGEXP_EXTRACT)
                        .calledWithArgumentTypes(DataTypes.STRING(), DataTypes.STRING())
                        .calledWithLiteralAt(1, null)
                        .expectArgumentTypes(DataTypes.STRING(), DataTypes.STRING()),

                // Invalid literal regex surfaces as a ValidationException at plan time
                // instead of producing one log line per record at runtime.
                TestSpec.forStrategy("Invalid literal regex fails at plan time", REGEXP_EXTRACT)
                        .calledWithArgumentTypes(DataTypes.STRING(), DataTypes.STRING())
                        .calledWithLiteralAt(1, "(")
                        .expectErrorMessage("Invalid regular expression for REGEXP_EXTRACT:"));
    }
}
