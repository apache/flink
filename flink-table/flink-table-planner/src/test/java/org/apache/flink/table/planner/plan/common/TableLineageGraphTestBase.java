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

package org.apache.flink.table.planner.plan.common;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

import net.javacrumbs.jsonunit.core.Option;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;

/** Lineage Graph tests for varies queries. */
public abstract class TableLineageGraphTestBase extends TableTestBase {
    private static final String RESOURCE_PATH = "src/test/resources/lineage-graph/";
    private final ObjectMapper mapper = new ObjectMapper();
    private TableTestUtil util;

    protected abstract boolean isBatchMode();

    protected abstract TableTestUtil getTableTestUtil();

    private final String query =
            "SELECT\n"
                    + "  AVG(a) AS avg_a,\n"
                    + "  COUNT(*) AS cnt,\n"
                    + "  count(b) AS cnt_b,\n"
                    + "  min(b) AS min_b,\n"
                    + "  MAX(c) FILTER (WHERE a > 1) AS max_c\n"
                    + "FROM FirstTable";

    private final String lookupJoin = "";

    private final String union =
            "("
                    + query
                    + ") UNION \n"
                    + "(SELECT\n"
                    + "  AVG(a) AS avg_a,\n"
                    + "  COUNT(*) AS cnt,\n"
                    + "  count(b) AS cnt_b,\n"
                    + "  min(b) AS min_b,\n"
                    + "  MAX(c) FILTER (WHERE a > 1) AS max_c\n"
                    + "FROM SecondTable)";

    @BeforeEach
    void setup() {
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        util = getTableTestUtil();

        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE FirstTable (\n"
                                + "  a BIGINT,\n"
                                + "  b INT NOT NULL,\n"
                                + "  c VARCHAR,\n"
                                + "  d BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = '"
                                + isBatchMode()
                                + "')");

        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE SecondTable (\n"
                                + "  a BIGINT,\n"
                                + "  b INT NOT NULL,\n"
                                + "  c VARCHAR,\n"
                                + "  d BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = '"
                                + isBatchMode()
                                + "')");

        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE SinkTable (\n"
                                + "  avg_a DOUBLE,\n"
                                + "  cnt BIGINT,\n"
                                + "  cnt_b BIGINT,\n"
                                + "  min_b BIGINT,\n"
                                + "  max_c VARCHAR\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false')");
    }

    @Test
    void testInsertWithSelect() throws Exception {
        List<Transformation<?>> transformations =
                util.generateTransformations(String.format("INSERT INTO SinkTable\n%s", query));
        LineageGraph lineageGraph = generateLineageGraph(transformations);
        verify(lineageGraph, isBatchMode() ? "query-batch.json" : "query-stream.json");
    }

    @Test
    void testInsertWithUnion() throws Exception {
        List<Transformation<?>> transformations =
                util.generateTransformations(String.format("INSERT INTO SinkTable\n%s", union));
        LineageGraph lineageGraph = generateLineageGraph(transformations);
        verify(lineageGraph, isBatchMode() ? "union-batch.json" : "union-stream.json");
    }

    private LineageGraph generateLineageGraph(List<Transformation<?>> transformations) {
        StreamGraphGenerator streamGraphGenerator =
                new StreamGraphGenerator(
                        transformations, new ExecutionConfig(), new CheckpointConfig());
        StreamGraph graph = streamGraphGenerator.generate();
        LineageGraph lineageGraph = graph.getLineageGraph();
        return lineageGraph;
    }

    private void verify(LineageGraph lineageGraph, String jsonPath) throws Exception {
        String json = mapper.writeValueAsString(lineageGraph);
        String expected = new String(Files.readAllBytes(Paths.get(RESOURCE_PATH + jsonPath)));
        assertThatJson(json).when(Option.IGNORING_ARRAY_ORDER).isEqualTo(expected);
    }
}
