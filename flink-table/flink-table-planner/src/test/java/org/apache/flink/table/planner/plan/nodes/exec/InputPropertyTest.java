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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.planner.utils.JsonTestUtils;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that physical nodes declare the input distribution requested by their physical rule in
 * their {@link InputProperty}s, so that a node reverting to {@link InputProperty#DEFAULT} is
 * caught.
 */
class InputPropertyTest {

    private static final String SINGLETON = "SINGLETON";
    private static final String UNKNOWN = "UNKNOWN";

    static Stream<Arguments> testCases() {
        return Stream.of(
                // window table functions request a distribution only for session windows
                Arguments.of(
                        "SELECT * FROM SESSION(TABLE Events, DESCRIPTOR(ts), INTERVAL '1' MINUTE)",
                        "stream-exec-window-table-function",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM SESSION(TABLE Events PARTITION BY k, DESCRIPTOR(ts), "
                                + "INTERVAL '1' MINUTE)",
                        "stream-exec-window-table-function",
                        List.of("HASH[1]")),
                Arguments.of(
                        "SELECT * FROM TUMBLE(TABLE Events, DESCRIPTOR(ts), INTERVAL '1' MINUTE)",
                        "stream-exec-window-table-function",
                        List.of(UNKNOWN)),
                // lookup joins request a distribution only for the hash shuffle of the LOOKUP hint
                Arguments.of(
                        "SELECT /*+ LOOKUP('table'='Dimension', 'shuffle'='true') */ e.i "
                                + "FROM Events e JOIN Dimension FOR SYSTEM_TIME AS OF e.proc "
                                + "ON e.k = Dimension.k",
                        "stream-exec-lookup-join",
                        List.of("HASH[1]")),
                Arguments.of(
                        "SELECT e.i FROM Events e JOIN Dimension FOR SYSTEM_TIME AS OF e.proc "
                                + "ON e.k = Dimension.k",
                        "stream-exec-lookup-join",
                        List.of(UNKNOWN)),
                // PTFs request a distribution for table arguments with set semantics
                Arguments.of(
                        "SELECT * FROM SetSemanticFunction(input => TABLE Dimension)",
                        "stream-exec-process-table-function",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM SetSemanticFunction("
                                + "input => TABLE Dimension PARTITION BY k)",
                        "stream-exec-process-table-function",
                        List.of("HASH[0]")),
                // nodes that always request a distribution
                Arguments.of(
                        "SELECT a.i, b.i AS j FROM Events a JOIN Events b ON a.k = b.k",
                        "stream-exec-join",
                        List.of("HASH[1]", "HASH[1]")),
                Arguments.of(
                        "SELECT COUNT(*) FROM Events",
                        "stream-exec-group-aggregate",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT k, COUNT(*) FROM Events GROUP BY k",
                        "stream-exec-group-aggregate",
                        List.of("HASH[0]")),
                Arguments.of(
                        "SELECT window_start, COUNT(*) FROM TUMBLE(TABLE Events, DESCRIPTOR(ts), "
                                + "INTERVAL '1' MINUTE) GROUP BY window_start, window_end",
                        "stream-exec-global-window-aggregate",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT k, window_start, COUNT(*) "
                                + "FROM TUMBLE(TABLE Events, DESCRIPTOR(ts), INTERVAL '1' MINUTE) "
                                + "GROUP BY k, window_start, window_end",
                        "stream-exec-global-window-aggregate",
                        List.of("HASH[0]")),
                Arguments.of(
                        "SELECT k, COUNT(*) FROM Events "
                                + "GROUP BY k, TUMBLE(ts, INTERVAL '1' MINUTE)",
                        "stream-exec-group-window-aggregate",
                        List.of("HASH[0]")),
                Arguments.of(
                        "SELECT i, COUNT(*) OVER (ORDER BY ts "
                                + "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM Events",
                        "stream-exec-over-aggregate",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT i, COUNT(*) OVER (PARTITION BY k ORDER BY ts "
                                + "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM Events",
                        "stream-exec-over-aggregate",
                        List.of("HASH[1]")),
                Arguments.of(
                        "SELECT * FROM (SELECT i, ROW_NUMBER() OVER (ORDER BY i) AS r FROM Events) "
                                + "WHERE r <= 2",
                        "stream-exec-rank",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM (SELECT i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) "
                                + "AS r FROM Events) WHERE r <= 2",
                        "stream-exec-rank",
                        List.of("HASH[1]")),
                Arguments.of(
                        "SELECT * FROM (SELECT i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY proc) "
                                + "AS r FROM Events) WHERE r = 1",
                        "stream-exec-deduplicate",
                        List.of("HASH[1]")),
                Arguments.of(
                        "SELECT * FROM Events ORDER BY i LIMIT 2",
                        "stream-exec-sort-limit",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM Events LIMIT 2", "stream-exec-limit", List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM Events ORDER BY ts",
                        "stream-exec-temporal-sort",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT a.i FROM Events a, Events b "
                                + "WHERE a.ts BETWEEN b.ts - INTERVAL '1' MINUTE AND b.ts",
                        "stream-exec-interval-join",
                        List.of(SINGLETON, SINGLETON)),
                Arguments.of(
                        "SELECT a.i FROM Events a, Events b WHERE a.k = b.k "
                                + "AND a.ts BETWEEN b.ts - INTERVAL '1' MINUTE AND b.ts",
                        "stream-exec-interval-join",
                        List.of("HASH[1]", "HASH[0]")),
                Arguments.of(
                        "SELECT e.i, v.v FROM Events e "
                                + "JOIN Versioned FOR SYSTEM_TIME AS OF e.ts AS v ON e.k = v.k",
                        "stream-exec-temporal-join",
                        List.of("HASH[1]", "HASH[0]")),
                Arguments.of(
                        "SELECT a.i FROM "
                                + "TUMBLE(TABLE Events, DESCRIPTOR(ts), INTERVAL '1' MINUTE) a "
                                + "JOIN "
                                + "TUMBLE(TABLE Events, DESCRIPTOR(ts), INTERVAL '1' MINUTE) b "
                                + "ON a.window_start = b.window_start "
                                + "AND a.window_end = b.window_end AND a.k = b.k",
                        "stream-exec-window-join",
                        List.of("HASH[1]", "HASH[0]")),
                Arguments.of(
                        "SELECT * FROM (SELECT i, ROW_NUMBER() OVER ("
                                + "PARTITION BY window_start, window_end ORDER BY i) AS r "
                                + "FROM TUMBLE(TABLE Events, DESCRIPTOR(ts), INTERVAL '1' MINUTE)) "
                                + "WHERE r <= 2",
                        "stream-exec-window-rank",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM (SELECT i, ROW_NUMBER() OVER ("
                                + "PARTITION BY window_start, window_end ORDER BY ts) AS r "
                                + "FROM TUMBLE(TABLE Events, DESCRIPTOR(ts), INTERVAL '1' MINUTE)) "
                                + "WHERE r = 1",
                        "stream-exec-window-deduplicate",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM Events MATCH_RECOGNIZE (ORDER BY ts MEASURES A.i AS ai "
                                + "PATTERN (A) DEFINE A AS A.i > 0)",
                        "stream-exec-match",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM Events MATCH_RECOGNIZE (PARTITION BY k ORDER BY ts "
                                + "MEASURES A.i AS ai PATTERN (A) DEFINE A AS A.i > 0)",
                        "stream-exec-match",
                        List.of("HASH[1]")),
                Arguments.of(
                        "SELECT * FROM Versioned WHERE v > 1",
                        "stream-exec-changelog-normalize",
                        List.of("HASH[0]")),
                // batch nodes with a strictly requested distribution
                Arguments.of(
                        "SELECT * FROM Events MATCH_RECOGNIZE (ORDER BY ts MEASURES A.i AS ai "
                                + "PATTERN (A) DEFINE A AS A.i > 0)",
                        "batch-exec-match",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM Events MATCH_RECOGNIZE (PARTITION BY k ORDER BY ts "
                                + "MEASURES A.i AS ai PATTERN (A) DEFINE A AS A.i > 0)",
                        "batch-exec-match",
                        List.of("HASH[1]")),
                Arguments.of(
                        "SELECT /*+ LOOKUP('table'='Dimension', 'shuffle'='true') */ e.i "
                                + "FROM Events e JOIN Dimension FOR SYSTEM_TIME AS OF e.proc "
                                + "ON e.k = Dimension.k",
                        "batch-exec-lookup-join",
                        List.of("HASH[1]")),
                // batch sorts only declare a singleton distribution
                Arguments.of(
                        "SELECT * FROM Events ORDER BY i", "batch-exec-sort", List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM (SELECT i, RANK() OVER (PARTITION BY k ORDER BY i) AS r "
                                + "FROM Events) WHERE r <= 2",
                        "batch-exec-sort",
                        List.of(UNKNOWN, UNKNOWN)));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void testRequestedInputDistribution(
            String query, String execNodeType, List<String> expectedDistributions)
            throws Exception {
        final boolean isBatch = execNodeType.startsWith("batch-");
        final TableEnvironment tEnv =
                TableEnvironment.create(
                        isBatch
                                ? EnvironmentSettings.inBatchMode()
                                : EnvironmentSettings.inStreamingMode());
        createTables(tEnv, isBatch);
        tEnv.createTemporaryTable(
                "S",
                TableDescriptor.forConnector("blackhole")
                        .schema(
                                Schema.newBuilder()
                                        .fromResolvedSchema(
                                                tEnv.sqlQuery(query).getResolvedSchema())
                                        .build())
                        .build());

        final JsonNode plan =
                JsonTestUtils.readFromString(
                        tEnv.compilePlanSql("INSERT INTO S " + query).asJsonString());

        assertThat(findInputDistributions(plan, execNodeType))
                .containsExactlyElementsOf(expectedDistributions);
    }

    /**
     * Creates the tables used by the test cases:
     *
     * <ul>
     *   <li>{@code Events}: append-only table with event time and processing time.
     *   <li>{@code Dimension}: append-only table, used as lookup table and as PTF input.
     *   <li>{@code Versioned}: upsert table with event time, used as versioned table and changelog
     *       input.
     * </ul>
     */
    private static void createTables(TableEnvironment tEnv, boolean isBatch) {
        final String appendOnly =
                String.format("WITH ('connector' = 'values', 'bounded' = '%s')", isBatch);
        final String upsert = "WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')";
        tEnv.executeSql(
                "CREATE TABLE Events (i INT, k STRING, ts TIMESTAMP(3), proc AS PROCTIME(), "
                        + "WATERMARK FOR ts AS ts) "
                        + appendOnly);
        tEnv.executeSql("CREATE TABLE Dimension (k STRING, v INT) " + appendOnly);
        tEnv.executeSql(
                "CREATE TABLE Versioned (k STRING, v INT, ts TIMESTAMP(3), "
                        + "PRIMARY KEY (k) NOT ENFORCED, WATERMARK FOR ts AS ts) "
                        + upsert);
        tEnv.createTemporarySystemFunction("SetSemanticFunction", SetSemanticFunction.class);
    }

    /** Returns the required distribution of each input of all nodes of the given type. */
    private static List<String> findInputDistributions(JsonNode plan, String execNodeType) {
        final List<String> distributions = new ArrayList<>();
        for (JsonNode node : plan.get("nodes")) {
            if (node.get("type").asText().startsWith(execNodeType + "_")) {
                for (JsonNode inputProperty : node.get("inputProperties")) {
                    distributions.add(toString(inputProperty.get("requiredDistribution")));
                }
            }
        }
        assertThat(distributions)
                .as("ExecNodes of type %s in plan %s", execNodeType, plan)
                .isNotEmpty();
        return distributions;
    }

    private static String toString(JsonNode distribution) {
        final String type = distribution.get("type").asText();
        return distribution.has("keys") ? type + distribution.get("keys") : type;
    }

    /** Process table function with a table argument that has set semantics. */
    public static class SetSemanticFunction extends ProcessTableFunction<Integer> {
        public void eval(
                @ArgumentHint({
                            ArgumentTrait.SET_SEMANTIC_TABLE,
                            ArgumentTrait.OPTIONAL_PARTITION_BY
                        })
                        Row input) {
            collect(input.getArity());
        }
    }
}
