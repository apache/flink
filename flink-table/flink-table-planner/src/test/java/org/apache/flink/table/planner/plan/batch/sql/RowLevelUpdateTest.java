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

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import scala.collection.Seq;

/** Test for row-level update. */
@RunWith(Parameterized.class)
public class RowLevelUpdateTest extends TableTestBase {

    private final Seq<ExplainDetail> explainDetails =
            JavaScalaConversionUtil.toScala(
                    Collections.singletonList(ExplainDetail.JSON_EXECUTION_PLAN));
    private final SupportsRowLevelUpdate.RowLevelUpdateMode updateMode;

    private BatchTableTestUtil util;

    @Parameterized.Parameters(name = "updateMode = {0}")
    public static Collection<SupportsRowLevelUpdate.RowLevelUpdateMode> data() {
        return Arrays.asList(
                SupportsRowLevelUpdate.RowLevelUpdateMode.UPDATED_ROWS,
                SupportsRowLevelUpdate.RowLevelUpdateMode.ALL_ROWS);
    }

    public RowLevelUpdateTest(SupportsRowLevelUpdate.RowLevelUpdateMode updateMode) {
        this.updateMode = updateMode;
    }

    @Before
    public void before() {
        util = batchTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 12);
    }

    @Test
    public void testUpdateWithoutFilter() {
        createTableForUpdate();
        util.verifyExplainInsert("UPDATE t SET b = 'n1', a = char_length(b) * a ", explainDetails);
    }

    @Test
    public void testUpdateWithFilter() {
        createTableForUpdate();
        util.verifyExplainInsert(
                "UPDATE t SET b = 'v2' WHERE a = 123 AND b = 'v1'", explainDetails);
    }

    @Test
    public void testUpdateWithSubQuery() {
        createTableForUpdate();
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE t1 (a int, b string) WITH "
                                        + "('connector' = 'test-update-delete', 'update-mode' = '%s') ",
                                updateMode));
        util.verifyExplainInsert(
                "UPDATE t SET b = 'v2' WHERE a = (SELECT count(*) FROM t1)", explainDetails);
    }

    @Test
    public void testUpdateAllColsWithOnlyRequireUpdatedCols() {
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE t (a int, b string, c double) WITH "
                                        + "('connector' = 'test-update-delete',"
                                        + " 'update-mode' = '%s',"
                                        + " 'only-require-updated-columns-for-update' = 'true'"
                                        + ") ",
                                updateMode));
        util.verifyExplainInsert(
                "UPDATE t SET b = 'v2', a = 123, c = c + 1 WHERE c > 123", explainDetails);
    }

    @Test
    public void testUpdatePartColsWithOnlyRequireUpdatedCols() {
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE t (f0 string, f1 int, a int, b string, c double, f2 string, f3 int) WITH "
                                        + "('connector' = 'test-update-delete',"
                                        + " 'update-mode' = '%s',"
                                        + " 'only-require-updated-columns-for-update' = 'true'"
                                        + ") ",
                                updateMode));
        util.verifyExplainInsert(
                "UPDATE t SET b = 'v2', a = 123, c = c + 1 WHERE c > 123", explainDetails);
    }

    @Test
    public void testUpdateWithCustomColumns() {
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE t (a int, b string, c double) WITH"
                                        + " ("
                                        + "'connector' = 'test-update-delete', "
                                        + "'required-columns-for-update' = 'b;c', "
                                        + "'update-mode' = '%s'"
                                        + ") ",
                                updateMode));
        util.verifyExplainInsert("UPDATE t SET b = 'v2' WHERE b = '123'", explainDetails);
    }

    @Test
    public void testUpdateWithMetaColumns() {
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE t (a int, b string, c double) WITH"
                                        + " ("
                                        + "'connector' = 'test-update-delete', "
                                        + "'required-columns-for-update' = 'meta_f1;meta_k2;a;b', "
                                        + "'update-mode' = '%s'"
                                        + ") ",
                                updateMode));
        util.verifyExplainInsert("UPDATE t SET b = 'v2' WHERE b = '123'", explainDetails);
    }

    @Test(expected = SqlParserException.class)
    public void testUpdateWithCompositeType() {
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE t ("
                                        + "a int,"
                                        + "b ROW<b1 STRING, b2 INT>,"
                                        + "c ROW<c1 BIGINT, c2 STRING>"
                                        + ") WITH ("
                                        + "'connector' = 'test-update-delete', "
                                        + "'update-mode' = '%s'"
                                        + ") ",
                                updateMode));
        util.verifyExplainInsert(
                "UPDATE t SET b.b1 = 'v2', c.c1 = 1000 WHERE b = '123'", explainDetails);
    }

    private void createTableForUpdate() {
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE t (a int, b string) WITH "
                                        + "('connector' = 'test-update-delete', 'update-mode' = '%s') ",
                                updateMode));
    }
}
