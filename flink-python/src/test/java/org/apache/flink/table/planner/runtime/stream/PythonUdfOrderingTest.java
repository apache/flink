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

package org.apache.flink.table.planner.runtime.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ParallelPythonScalarFunction;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests ordering around Python operators with explicit concurrency. */
class PythonUdfOrderingTest extends TableTestBase {
    private StreamTableTestUtil util;

    @BeforeEach
    void setUp() {
        util = streamTestUtil(TableConfig.getDefault());
        util.getTableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE src (rt TIMESTAMP(3), v INT, "
                                + "WATERMARK FOR rt AS rt - INTERVAL '1' SECOND) "
                                + "WITH ('connector' = 'values')");
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE sink_table (v INT, p INT) WITH "
                                + "('connector' = 'values', 'sink-insert-only' = 'false')");
        util.addTemporarySystemFunction("f", new ParallelPythonScalarFunction("f", 2));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTemporalSortFollowsConcurrentPython(boolean projectRowtime) {
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE ordered_sink ("
                                + (projectRowtime ? "rt TIMESTAMP(3), " : "")
                                + "p INT) WITH ('connector' = 'values')");
        final Transformation<?> root =
                translate(
                        "INSERT INTO ordered_sink SELECT "
                                + (projectRowtime ? "rt, " : "")
                                + "f(v, v) FROM src ORDER BY rt, v");
        final Transformation<?> sort = find(root, "TemporalSort");
        final Transformation<?> python = find(root, "PythonCalc");
        assertThat(sort.getParallelism()).isEqualTo(1);
        assertThat(python.getParallelism()).isEqualTo(2);
        assertThat(python.isParallelismConfigured()).isTrue();
        assertThat(sort.getTransitivePredecessors()).contains(python);
        assertThat(python.getTransitivePredecessors()).doesNotContain(sort);
    }

    @Test
    void testTopNFollowsConcurrentPython() {
        final Transformation<?> root =
                translate(
                        "INSERT INTO sink_table SELECT v, f(v, v) FROM src ORDER BY v DESC LIMIT 3");
        final Transformation<?> rank = find(root, "SortLimit");
        final Transformation<?> python = find(root, "PythonCalc");
        assertThat(python.getParallelism()).isEqualTo(2);
        assertThat(rank.getTransitivePredecessors()).contains(python);
    }

    private Transformation<?> translate(String statement) {
        return util.getPlanner()
                .translate(
                        Collections.singletonList(
                                (ModifyOperation)
                                        util.getPlanner().getParser().parse(statement).get(0)))
                .get(0);
    }

    private static Transformation<?> find(Transformation<?> root, String name) {
        return root.getTransitivePredecessors().stream()
                .filter(node -> node.getName().startsWith(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing " + name + " in " + root));
    }
}
