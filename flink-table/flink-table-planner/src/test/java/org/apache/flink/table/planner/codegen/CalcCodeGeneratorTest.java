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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.planner.codegen.JsonParseReuseTest.JSON_ROW1;
import static org.apache.flink.table.planner.codegen.JsonParseReuseTest.JSON_ROW2;
import static org.apache.flink.table.planner.codegen.JsonParseReuseTest.LIKE_X;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the code generated for a Calc. */
class CalcCodeGeneratorTest {

    private StreamTableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        tEnv =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        EnvironmentSettings.inStreamingMode());
        tEnv.createTemporaryView(
                "json_src",
                tEnv.fromValues(Row.of(JSON_ROW1, "{}"), Row.of(JSON_ROW2, "{\"x\":1}"))
                        .as("json_data", "other_json"));
    }

    @Test
    void testNonDeterministicValueIsNotMemoized() {
        // RAND() directly in both branches keeps it in one Calc, so RAND() + 1 is one ref
        final String sql =
                "SELECT CASE WHEN a THEN RAND() + 1 END, CASE WHEN a THEN (RAND() + 1) * 2 END "
                        + "FROM (SELECT "
                        + LIKE_X
                        + " AS a FROM json_src)";
        assertThat(GeneratedCodeTestUtils.collect(tEnv, sql)).hasSize(2);
        assertThat(GeneratedCodeTestUtils.generatedCode(tEnv, sql))
                .as("RAND() must not be computed in a memo method")
                .doesNotContain("private void localRef$");
    }
}
