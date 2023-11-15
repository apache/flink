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

package org.apache.flink.table.planner.plan.optimize;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assumptions.assumeThat;

/** Test push project into source with sub plan reuse. */
@ExtendWith(ParameterizedTestExtension.class)
class ScanReuseTest extends TableTestBase {

    private final boolean isStreaming;
    private final TableTestUtil util;

    ScanReuseTest(boolean isStreaming) {
        this.isStreaming = isStreaming;
        TableConfig config = TableConfig.getDefault();
        this.util = isStreaming ? streamTestUtil(config) : batchTestUtil(config);
    }

    @Parameters(name = "isStreaming: {0}")
    private static Collection<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    @BeforeEach
    void before() {
        String table =
                isStreaming
                        ? "CREATE TABLE MyTable (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  nested ROW<i int, j int, s string>,\n"
                                + "  metadata_1 int,\n"
                                + "  compute_metadata as metadata_1 * 2,\n"
                                + "  metadata_2 int,\n"
                                + "  rtime as TO_TIMESTAMP(c, nested.s),\n"
                                + "  WATERMARK FOR rtime AS rtime - INTERVAL '5' SECOND\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'false',\n"
                                + " 'nested-projection-supported' = 'true',\n"
                                + " 'readable-metadata' = 'metadata_1:INT, metadata_2:STRING',\n"
                                + " 'enable-watermark-push-down' = 'true',\n"
                                + " 'disable-lookup' = 'true'"
                                + ")"
                        : "CREATE TABLE MyTable (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  nested ROW<i int, j int, s string>,\n"
                                + "  metadata_1 int,\n"
                                + "  compute_metadata as metadata_1 * 2,\n"
                                + "  metadata_2 int\n"
                                + ") PARTITIONED BY (c) WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true',\n"
                                + " 'nested-projection-supported' = 'true',\n"
                                + " 'readable-metadata' = 'metadata_1:INT, metadata_2:STRING'\n"
                                + ")";
        util.tableEnv().executeSql(table);
    }

    @TestTemplate
    void testProject() {
        String sqlQuery = "SELECT T1.a, T1.c, T2.c FROM MyTable T1, MyTable T2 WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProject1() {
        // One side projection
        String sqlQuery =
                "SELECT T1.a, T1.b, T1.c, T2.c FROM MyTable T1, MyTable T2 WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProject2() {
        // Two side projection
        String sqlQuery = "SELECT T1.a, T1.b, T2.c FROM MyTable T1, MyTable T2 WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectNested1() {
        String sqlQuery =
                "SELECT T1.a, T1.i, T2.j FROM"
                        + " (SELECT a, nested.i as i FROM MyTable) T1,"
                        + " (SELECT a, nested.j as j FROM MyTable) T2 WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectNested2() {
        String sqlQuery =
                "SELECT T1.a, T1.i, T2.i FROM"
                        + " (SELECT a, nested.i as i FROM MyTable) T1,"
                        + " (SELECT a, nested.i as i FROM MyTable) T2 WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectNestedWithWholeField() {
        String sqlQuery =
                "SELECT * FROM"
                        + " (SELECT a, nested.i FROM MyTable) T1,"
                        + " (SELECT a, nested FROM MyTable) T2 WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectWithExpr() {
        String sqlQuery =
                "SELECT T1.a, T1.b, T2.c FROM"
                        + " (SELECT a, b + 1 as b FROM MyTable) T1, MyTable T2 WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectWithFilter() {
        String sqlQuery =
                "SELECT T1.a, T1.b, T2.c FROM"
                        + " (SELECT * FROM MyTable WHERE b = 2) T1,"
                        + " (SELECT * FROM MyTable WHERE b = 3) T2 WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectWithMeta1() {
        // One side meta
        String sqlQuery =
                "SELECT T1.a, T1.b, T1.metadata_1, T1.metadata_2, T2.c, T2.metadata_2"
                        + " FROM MyTable T1, MyTable T2 WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectWithMeta2() {
        // One side meta
        String sqlQuery =
                "SELECT T1.a, T1.b, T1.metadata_1, T2.c, T2.metadata_2"
                        + " FROM MyTable T1, MyTable T2 WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectWithMeta3() {
        // meta projection
        String sqlQuery =
                "SELECT T1.a, T1.b, T1.metadata_1, T2.c, T2.metadata_1"
                        + " FROM MyTable T1, MyTable T2 WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectWithMetaAndCompute() {
        String sqlQuery =
                "SELECT T1.a, T1.b, T1.metadata_1, T1.compute_metadata, T2.c, T2.metadata_2"
                        + " FROM MyTable T1, MyTable T2 WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectWithHints() {
        String sqlQuery =
                "SELECT T1.a, T1.c, T2.c FROM"
                        + " MyTable /*+ OPTIONS('source.num-element-to-skip'='1') */ T1,"
                        + " MyTable T2"
                        + " WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectReuseWithHints() {
        String sqlQuery =
                "SELECT T1.a, T1.c, T2.c FROM"
                        + " MyTable /*+ OPTIONS('source.num-element-to-skip'='1') */ T1,"
                        + " MyTable /*+ OPTIONS('source.num-element-to-skip'='1') */ T2"
                        + " WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectWithDifferentHints() {
        String sqlQuery =
                "SELECT T1.a, T1.c, T2.c FROM"
                        + " MyTable /*+ OPTIONS('source.num-element-to-skip'='1') */ T1,"
                        + " MyTable /*+ OPTIONS('source.num-element-to-skip'='10') */ T2"
                        + " WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectWithFilterPushDown() {
        String sqlQuery =
                "SELECT T1.a, T1.c, T2.c FROM"
                        + " (SELECT * FROM"
                        + " MyTable /*+ OPTIONS('filterable-fields'='b') */ WHERE b = 2) T1,"
                        + " (SELECT * FROM"
                        + " MyTable /*+ OPTIONS('filterable-fields'='b') */ WHERE b = 1) T2"
                        + " WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectReuseWithFilterPushDown() {
        String sqlQuery =
                "SELECT T1.a, T1.c, T2.c FROM"
                        + " (SELECT * FROM"
                        + " MyTable /*+ OPTIONS('filterable-fields'='b') */ WHERE b = 1) T1,"
                        + " (SELECT * FROM"
                        + " MyTable /*+ OPTIONS('filterable-fields'='b') */ WHERE b = 1) T2"
                        + " WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectReuseWithWatermark() {
        if (isStreaming) {
            String ddl =
                    "CREATE TABLE W_T (\n"
                            + "  a int,\n"
                            + "  b bigint,\n"
                            + "  c string,\n"
                            + "  rtime timestamp(3),\n"
                            + "  WATERMARK FOR rtime AS rtime - INTERVAL '5' SECOND\n"
                            + ") WITH (\n"
                            + " 'connector' = 'values',\n"
                            + " 'bounded' = 'false',\n"
                            + " 'enable-watermark-push-down' = 'true',\n"
                            + " 'disable-lookup' = 'true'"
                            + ")";
            util.tableEnv().executeSql(ddl);
            String sqlQuery =
                    "SELECT T1.a, T1.c, T2.b FROM"
                            + " (SELECT MIN(a) as a, MIN(c) as c FROM W_T GROUP BY"
                            + " TUMBLE(rtime, INTERVAL '10' SECOND)) T1,"
                            + " (SELECT MIN(a) as a, MIN(b) as b FROM W_T GROUP BY"
                            + " TUMBLE(rtime, INTERVAL '10' SECOND)) T2"
                            + " WHERE T1.a = T2.a";
            util.verifyExecPlan(sqlQuery);
        }
    }

    @TestTemplate
    void testProjectWithLimitPushDown() {
        String sqlQuery =
                "SELECT T1.a, T1.c, T2.c FROM"
                        + " (SELECT * FROM"
                        + " MyTable LIMIT 11) T1,"
                        + " (SELECT * FROM"
                        + " MyTable LIMIT 10) T2"
                        + " WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectReuseWithLimitPushDown() {
        String sqlQuery =
                "SELECT T1.a, T1.c, T2.c FROM"
                        + " (SELECT * FROM"
                        + " MyTable LIMIT 10) T1,"
                        + " (SELECT * FROM"
                        + " MyTable LIMIT 10) T2"
                        + " WHERE T1.a = T2.a";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testProjectWithPartitionPushDown() {
        if (!isStreaming) {
            String sqlQuery =
                    "SELECT T1.a, T1.c, T2.c FROM"
                            + " (SELECT * FROM"
                            + " MyTable /*+ OPTIONS('partition-list'='c:1;c:2') */"
                            + " WHERE c = '1') T1,"
                            + " (SELECT * FROM"
                            + " MyTable /*+ OPTIONS('partition-list'='c:1;c:2') */"
                            + " WHERE c = '2') T2"
                            + " WHERE T1.a = T2.a";
            util.verifyExecPlan(sqlQuery);
        }
    }

    @TestTemplate
    void testProjectReuseWithPartitionPushDown() {
        if (!isStreaming) {
            String sqlQuery =
                    "SELECT T1.a, T1.c, T2.c FROM"
                            + " (SELECT * FROM"
                            + " MyTable /*+ OPTIONS('partition-list'='c:1;c:2') */"
                            + " WHERE c = '1') T1,"
                            + " (SELECT * FROM"
                            + " MyTable /*+ OPTIONS('partition-list'='c:1;c:2') */"
                            + " WHERE c = '1') T2"
                            + " WHERE T1.a = T2.a";
            util.verifyExecPlan(sqlQuery);
        }
    }

    @TestTemplate
    void testReuseWithReadMetadataAndWatermarkPushDown1() {
        assumeThat(isStreaming).isTrue();
        String ddl =
                "CREATE TABLE MyTable1 (\n"
                        + "  metadata_0 int METADATA VIRTUAL,\n"
                        + "  a0 int,\n"
                        + "  a1 int,\n"
                        + "  a2 int,\n"
                        + "  ts STRING,\n "
                        + "  rowtime as TO_TIMESTAMP(`ts`),\n"
                        + "  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'false',\n"
                        + " 'readable-metadata' = 'metadata_0:int',\n"
                        + " 'enable-watermark-push-down' = 'true',\n"
                        + " 'disable-lookup' = 'true'"
                        + ")";
        util.tableEnv().executeSql(ddl);

        // join left side value source without projection spec.
        String sqlQuery =
                "SELECT T1.a1, T1.a2 FROM"
                        + " (SELECT a0, window_start, window_end,"
                        + " MIN(a1) as a1, MIN(a2) as a2, MIN(metadata_0) as metadata_0"
                        + " FROM TABLE("
                        + "   TUMBLE(TABLE MyTable1, DESCRIPTOR(rowtime), INTERVAL '1' SECOND)) "
                        + " GROUP BY a0, window_start, window_end) T1,"
                        + " (SELECT a0, window_start, window_end, MIN(a1) as a1"
                        + "  FROM TABLE("
                        + "   TUMBLE(TABLE MyTable1, DESCRIPTOR(rowtime), INTERVAL '1' SECOND)) "
                        + " GROUP BY a0, window_start, window_end) T2"
                        + " WHERE T1.a1 = T2.a1";
        util.verifyExecPlan(sqlQuery);
    }

    @TestTemplate
    void testReuseWithReadMetadataAndWatermarkPushDown2() {
        assumeThat(isStreaming).isTrue();
        String ddl =
                "CREATE TABLE MyTable1 (\n"
                        + "  metadata_0 int METADATA VIRTUAL,\n"
                        + "  a0 int,\n"
                        + "  a1 int,\n"
                        + "  a2 int,\n"
                        + "  ts STRING,\n "
                        + "  rowtime as TO_TIMESTAMP(`ts`),\n"
                        + "  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'false',\n"
                        + " 'readable-metadata' = 'metadata_0:int',\n"
                        + " 'enable-watermark-push-down' = 'true',\n"
                        + " 'disable-lookup' = 'true'"
                        + ")";
        util.tableEnv().executeSql(ddl);

        // join right side value source without projection spec.
        String sqlQuery =
                "SELECT T1.a1, T2.a2 FROM"
                        + " (SELECT a0, window_start, window_end, MIN(a1) as a1"
                        + "  FROM TABLE("
                        + "   TUMBLE(TABLE MyTable1, DESCRIPTOR(rowtime), INTERVAL '1' SECOND)) "
                        + " GROUP BY a0, window_start, window_end) T1,"
                        + " (SELECT a0, window_start, window_end,"
                        + " MIN(a1) as a1, MIN(a2) as a2, MIN(metadata_0) as metadata_0"
                        + " FROM TABLE("
                        + "   TUMBLE(TABLE MyTable1, DESCRIPTOR(rowtime), INTERVAL '1' SECOND)) "
                        + " GROUP BY a0, window_start, window_end) T2"
                        + " WHERE T1.a1 = T2.a1";
        util.verifyExecPlan(sqlQuery);
    }
}
