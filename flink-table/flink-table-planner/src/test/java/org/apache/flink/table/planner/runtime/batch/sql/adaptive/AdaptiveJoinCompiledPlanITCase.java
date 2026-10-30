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

package org.apache.flink.table.planner.runtime.batch.sql.adaptive;

import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for compiling and restoring a plan with an adaptive join. */
class AdaptiveJoinCompiledPlanITCase extends BatchTestBase {

    @Test
    void testCompiledPlanRoundTrip() throws Exception {
        final TableEnvironment tEnv = tEnv();
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE t1 (a INT, b BIGINT) WITH ("
                                + "'connector' = 'values', 'bounded' = 'true', 'data-id' = '%s')",
                        TestValuesTableFactory.registerData(
                                Arrays.asList(Row.of(1, 10L), Row.of(2, 20L), Row.of(3, 30L)))));
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE t2 (x INT, y STRING) WITH ("
                                + "'connector' = 'values', 'bounded' = 'true', 'data-id' = '%s')",
                        TestValuesTableFactory.registerData(
                                Arrays.asList(
                                        Row.of(1, "one"), Row.of(3, "three"), Row.of(4, "four")))));
        tEnv.executeSql(
                "CREATE TABLE sink_t (a INT, b BIGINT, y STRING) WITH ("
                        + "'connector' = 'values', 'sink-insert-only' = 'false')");

        final String compiledJson =
                tEnv.compilePlanSql("INSERT INTO sink_t SELECT a, b, y FROM t1 JOIN t2 ON a = x")
                        .asJsonString();
        final List<JsonNode> compiledJoins = adaptiveJoinNodes(compiledJson);
        assertThat(compiledJoins).hasSize(1);

        final CompiledPlan restoredPlan = tEnv.loadPlan(PlanReference.fromJsonString(compiledJson));
        // The description is serialized and read back, so it must not change on restore
        assertThat(adaptiveJoinNodes(restoredPlan.asJsonString()))
                .singleElement()
                .satisfies(
                        node ->
                                assertThat(node.get("description").asText())
                                        .isEqualTo(
                                                compiledJoins.get(0).get("description").asText()));

        restoredPlan.execute().await();
        assertThat(TestValuesTableFactory.getResultsAsStrings("sink_t"))
                .containsExactlyInAnyOrder("+I[1, 10, one]", "+I[3, 30, three]");
    }

    private static List<JsonNode> adaptiveJoinNodes(String json) throws Exception {
        final List<JsonNode> nodes = new ArrayList<>();
        for (JsonNode node : new ObjectMapper().readTree(json).get("nodes")) {
            if (node.get("type").asText().equals("batch-exec-adaptive-join_1")) {
                nodes.add(node);
            }
        }
        return nodes;
    }
}
