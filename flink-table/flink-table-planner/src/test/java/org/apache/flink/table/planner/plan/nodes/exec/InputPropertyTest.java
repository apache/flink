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
 * their {@link InputProperty}s. Covers nodes that request a distribution only conditionally.
 */
class InputPropertyTest {

    private static final String SINGLETON = "SINGLETON";
    private static final String UNKNOWN = "UNKNOWN";

    static Stream<Arguments> testCases() {
        return Stream.of(
                // window table functions request a distribution only for session windows
                Arguments.of(
                        "SELECT * FROM SESSION(TABLE T, DESCRIPTOR(ts), INTERVAL '1' MINUTE)",
                        "stream-exec-window-table-function",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM SESSION(TABLE T PARTITION BY k, DESCRIPTOR(ts), "
                                + "INTERVAL '1' MINUTE)",
                        "stream-exec-window-table-function",
                        List.of("HASH[1]")),
                Arguments.of(
                        "SELECT * FROM TUMBLE(TABLE T, DESCRIPTOR(ts), INTERVAL '1' MINUTE)",
                        "stream-exec-window-table-function",
                        List.of(UNKNOWN)),
                // lookup joins request a distribution only for the hash shuffle of the LOOKUP hint
                Arguments.of(
                        "SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.i FROM T "
                                + "JOIN D FOR SYSTEM_TIME AS OF T.proc ON T.k = D.k",
                        "stream-exec-lookup-join",
                        List.of("HASH[1]")),
                Arguments.of(
                        "SELECT T.i FROM T JOIN D FOR SYSTEM_TIME AS OF T.proc ON T.k = D.k",
                        "stream-exec-lookup-join",
                        List.of(UNKNOWN)),
                // PTFs request a distribution for table arguments with set semantics
                Arguments.of(
                        "SELECT * FROM SetSemanticFunction(input => TABLE U)",
                        "stream-exec-process-table-function",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM SetSemanticFunction(input => TABLE U PARTITION BY k)",
                        "stream-exec-process-table-function",
                        List.of("HASH[0]")),
                // nodes that always request a distribution
                Arguments.of(
                        "SELECT a.i, b.i AS j FROM T a JOIN T b ON a.k = b.k",
                        "stream-exec-join",
                        List.of("HASH[1]", "HASH[1]")),
                // batch nodes with a strictly requested distribution
                Arguments.of(
                        "SELECT * FROM T MATCH_RECOGNIZE (ORDER BY ts MEASURES A.i AS ai "
                                + "PATTERN (A) DEFINE A AS A.i > 0)",
                        "batch-exec-match",
                        List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM T MATCH_RECOGNIZE (PARTITION BY k ORDER BY ts "
                                + "MEASURES A.i AS ai PATTERN (A) DEFINE A AS A.i > 0)",
                        "batch-exec-match",
                        List.of("HASH[1]")),
                Arguments.of(
                        "SELECT /*+ LOOKUP('table'='D', 'shuffle'='true') */ T.i FROM T "
                                + "JOIN D FOR SYSTEM_TIME AS OF T.proc ON T.k = D.k",
                        "batch-exec-lookup-join",
                        List.of("HASH[1]")),
                // batch sorts only declare a singleton distribution
                Arguments.of("SELECT * FROM T ORDER BY i", "batch-exec-sort", List.of(SINGLETON)),
                Arguments.of(
                        "SELECT * FROM (SELECT i, RANK() OVER (PARTITION BY k ORDER BY i) AS r "
                                + "FROM T) WHERE r <= 2",
                        "batch-exec-sort",
                        List.of(UNKNOWN)));
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

    private static void createTables(TableEnvironment tEnv, boolean isBatch) {
        final String options =
                String.format("WITH ('connector' = 'values', 'bounded' = '%s')", isBatch);
        tEnv.executeSql(
                "CREATE TABLE T (i INT, k STRING, ts TIMESTAMP(3), proc AS PROCTIME(), "
                        + "WATERMARK FOR ts AS ts) "
                        + options);
        tEnv.executeSql("CREATE TABLE D (k STRING, v INT) " + options);
        tEnv.executeSql("CREATE TABLE U (k STRING, v INT) " + options);
        tEnv.createTemporarySystemFunction("SetSemanticFunction", SetSemanticFunction.class);
    }

    /** Returns the required distribution of each input of the first node of the given type. */
    private static List<String> findInputDistributions(JsonNode plan, String execNodeType) {
        for (JsonNode node : plan.get("nodes")) {
            if (node.get("type").asText().startsWith(execNodeType + "_")) {
                final List<String> distributions = new ArrayList<>();
                for (JsonNode inputProperty : node.get("inputProperties")) {
                    distributions.add(toString(inputProperty.get("requiredDistribution")));
                }
                return distributions;
            }
        }
        throw new AssertionError("No ExecNode of type " + execNodeType + " in plan: " + plan);
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
