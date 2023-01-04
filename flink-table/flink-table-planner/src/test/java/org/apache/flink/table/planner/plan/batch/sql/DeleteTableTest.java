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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/** Test for delete table statement. */
@RunWith(Parameterized.class)
public class DeleteTableTest extends TableTestBase {

    private final SupportsRowLevelDelete.RowLevelDeleteMode deleteMode;

    private BatchTableTestUtil util;

    @Parameterized.Parameters(name = "deleteMode = {0}")
    public static Collection<SupportsRowLevelDelete.RowLevelDeleteMode> data() {
        return Arrays.asList(
                SupportsRowLevelDelete.RowLevelDeleteMode.DELETED_ROWS,
                SupportsRowLevelDelete.RowLevelDeleteMode.REMAINING_ROWS);
    }

    public DeleteTableTest(
            SupportsRowLevelDelete.RowLevelDeleteMode deleteMode) {
        this.deleteMode = deleteMode;
    }

    @Before
    public void before() {
        util = batchTestUtil(TableConfig.getDefault());
    }

    @Test
    public void testDeleteWithoutFilter() {
        createDeleteTable();
        util.verifyExplainInsert("DELETE FROM t");
    }

    @Test
    public void testDeleteWithFilter() {
        createDeleteTable();
        util.verifyExplainInsert("DELETE FROM t where a = 1 and b = '123'");
    }

    @Test
    public void testDeleteWithSubQuery() {
        createDeleteTable();
        util.verifyExplainInsert(
                "DELETE FROM t where b = '123' and a = (select count(*) from t)");
    }

    @Test
    public void testDeleteWithCustomColumns() {
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE t (a int, b string, c double) WITH"
                                        + " ("
                                        + "'connector' = 'test-update-delete', "
                                        + "'required-columns-for-delete' = 'b;c', "
                                        + "'delete-mode' = '%s'"
                                        + ") ",
                                deleteMode));
        util.verifyExplainInsert("DELETE FROM t where b = '123'");
    }

    @Test
    public void testDeleteWithMetaColumns() {
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE t (a int, b string, c double) WITH"
                                        + " ("
                                        + "'connector' = 'test-update-delete', "
                                        + "'required-columns-for-delete' = 'meta_f1;meta_k2;b', "
                                        + "'delete-mode' = '%s'"
                                        + ") ",
                                deleteMode));
        util.verifyExplainInsert("DELETE FROM t where b = '123'");
    }

    private void createDeleteTable() {
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE t (a int, b string) WITH "
                                        + "('connector' = 'test-update-delete', 'delete-mode' = '%s') ",
                                deleteMode));
    }
}
