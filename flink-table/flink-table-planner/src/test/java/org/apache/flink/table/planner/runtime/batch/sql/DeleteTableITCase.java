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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.factories.TestUpdateDeleteTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The IT case for DELETE statement in batch mode. */
@RunWith(Parameterized.class)
public class DeleteTableITCase extends BatchTestBase {
    private static final int ROW_NUM = 5;

    private final SupportsRowLevelDelete.RowLevelDeleteMode deleteMode;

    @Parameterized.Parameters(name = "deleteMode = {0}")
    public static Collection<SupportsRowLevelDelete.RowLevelDeleteMode> data() {
        return Arrays.asList(
                SupportsRowLevelDelete.RowLevelDeleteMode.DELETED_ROWS,
                SupportsRowLevelDelete.RowLevelDeleteMode.REMAINING_ROWS);
    }

    public DeleteTableITCase(SupportsRowLevelDelete.RowLevelDeleteMode deleteMode) {
        this.deleteMode = deleteMode;
    }

    @Test
    public void testDeletePushDown() throws Exception {
        String dataId = registerData();
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE t (a int, b string, c double) WITH"
                                        + " ('connector' = 'test-update-delete',"
                                        + " 'data-id' = '%s',"
                                        + " 'only-accept-equal-predicate' = 'true'"
                                        + ")",
                                dataId));
        // it only contains equal expression, should be pushed down
        List<Row> rows = toRows(tEnv().executeSql("DELETE FROM t where a = 1"));
        assertThat(rows.toString()).isEqualTo("[+I[1]]");
        rows = toRows(tEnv().executeSql("SELECT * FROM t"));
        assertThat(rows.toString())
                .isEqualTo("[+I[0, b_0, 0.0], +I[2, b_2, 4.0], +I[3, b_3, 6.0], +I[4, b_4, 8.0]]");

        // should throw exception for the deletion can not be pushed down as it contains non-equal
        // predicate and the table sink haven't implemented SupportsRowLevelDeleteInterface
        assertThatThrownBy(() -> tEnv().executeSql("DELETE FROM t where a > 1"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        String.format(
                                "Can't perform delete operation of the table %s because the corresponding dynamic table sink has not yet implemented %s.",
                                "default_catalog.default_database.t",
                                SupportsRowLevelDelete.class.getName()));
    }

    @Test
    public void testRowLevelDelete() throws Exception {
        String dataId = registerData();
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE t (a int PRIMARY KEY NOT ENFORCED,"
                                        + " b string, c double) WITH"
                                        + " ('connector' = 'test-update-delete',"
                                        + " 'data-id' = '%s',"
                                        + " 'delete-mode' = '%s',"
                                        + " 'support-delete-push-down' = 'false'"
                                        + ")",
                                dataId, deleteMode));
        tEnv().executeSql("DELETE FROM t WHERE a > 1").await();
        List<Row> rows = toRows(tEnv().executeSql("SELECT * FROM t"));
        assertThat(rows.toString()).isEqualTo("[+I[0, b_0, 0.0], +I[1, b_1, 2.0]]");

        tEnv().executeSql("DELETE FROM t WHERE a >= (select count(1) from t where c > 1)").await();
        rows = toRows(tEnv().executeSql("SELECT * FROM t"));
        assertThat(rows.toString()).isEqualTo("[+I[0, b_0, 0.0]]");
    }

    @Test
    public void testRowLevelDeleteWithPartitionColumn() throws Exception {
        String dataId = registerData();
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE t"
                                        + " (a int PRIMARY KEY NOT ENFORCED,"
                                        + " b string not null,"
                                        + " c double not null) WITH"
                                        + " ('connector' = 'test-update-delete',"
                                        + " 'data-id' = '%s',"
                                        + " 'delete-mode' = '%s',"
                                        + " 'required-columns-for-delete' = 'a;c',"
                                        + " 'support-delete-push-down' = 'false'"
                                        + ")",
                                dataId, deleteMode));
        tEnv().executeSql("DELETE FROM t WHERE a > 1").await();
        List<Row> rows = toRows(tEnv().executeSql("SELECT * FROM t"));
        assertThat(rows.toString()).isEqualTo("[+I[0, b_0, 0.0], +I[1, b_1, 2.0]]");

        // test delete with requiring partial primary keys
        dataId = registerData();
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE t1"
                                        + " (a int,"
                                        + " b string not null,"
                                        + " c double not null,"
                                        + " PRIMARY KEY (a, c) NOT ENFORCED) WITH"
                                        + " ('connector' = 'test-update-delete',"
                                        + " 'data-id' = '%s',"
                                        + " 'delete-mode' = '%s',"
                                        + " 'required-columns-for-delete' = 'a;b',"
                                        + " 'support-delete-push-down' = 'false'"
                                        + ")",
                                dataId, deleteMode));
        tEnv().executeSql("DELETE FROM t1 WHERE a > 1").await();
        rows = toRows(tEnv().executeSql("SELECT * FROM t1"));
        assertThat(rows.toString()).isEqualTo("[+I[0, b_0, 0.0], +I[1, b_1, 2.0]]");
    }

    @Test
    public void testMixDelete() throws Exception {
        // test mix delete push down and row-level delete
        String dataId = registerData();
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE t (a int PRIMARY KEY NOT ENFORCED,"
                                        + " b string, c double) WITH"
                                        + " ('connector' = 'test-update-delete',"
                                        + " 'data-id' = '%s',"
                                        + " 'mix-delete' = 'true')",
                                dataId));
        // the deletion can't be pushed down, but the deletion should still success.
        tEnv().executeSql("DELETE FROM t WHERE a >= (select count(1) from t where c > 2)").await();
        List<Row> rows = toRows(tEnv().executeSql("SELECT * FROM t"));
        assertThat(rows.toString())
                .isEqualTo("[+I[0, b_0, 0.0], +I[1, b_1, 2.0], +I[2, b_2, 4.0]]");

        // should fall back to delete push down
        rows = toRows(tEnv().executeSql("DELETE FROM t"));
        assertThat(rows.toString()).isEqualTo("[+I[3]]");
        rows = toRows(tEnv().executeSql("SELECT * FROM t"));
        assertThat(rows).isEmpty();
    }

    @Test
    public void testStatementSetContainDeleteAndInsert() throws Exception {
        tEnv().executeSql(
                        "CREATE TABLE t (a int, b string, c double) WITH"
                                + " ('connector' = 'test-update-delete')");
        StatementSet statementSet = tEnv().createStatementSet();
        // should throw exception when statement set contains insert and delete statement
        statementSet.addInsertSql("INSERT INTO t VALUES (1, 'v1', 1)");
        statementSet.addInsertSql("DELETE FROM t");
        assertThatThrownBy(statementSet::execute)
                .isInstanceOf(TableException.class)
                .hasMessage(
                        "Unsupported SQL query! Only accept a single SQL statement of type DELETE.");
    }

    @Test
    public void testCompilePlanSql() throws Exception {
        tEnv().executeSql(
                        "CREATE TABLE t (a int, b string, c double) WITH"
                                + " ('connector' = 'test-update-delete')");
        // should throw exception when compile sql for delete statement
        assertThatThrownBy(() -> tEnv().compilePlanSql("DELETE FROM t"))
                .isInstanceOf(TableException.class)
                .hasMessage(
                        "Unsupported SQL query! compilePlanSql() only accepts a single SQL statement of type INSERT");
    }

    @Test
    public void testDeleteWithLegacyTableSink() {
        tEnv().executeSql(
                        "CREATE TABLE t (a int, b string, c double) WITH"
                                + " ('connector' = 'COLLECTION')");
        assertThatThrownBy(() -> tEnv().executeSql("DELETE FROM t"))
                .isInstanceOf(TableException.class)
                .hasMessage(
                        String.format(
                                "Can't perform delete operation of the table %s "
                                        + " because the corresponding table sink is the legacy TableSink,"
                                        + " Please implement %s for it.",
                                "`default_catalog`.`default_database`.`t`",
                                DynamicTableSink.class.getName()));
    }

    private String registerData() {
        List<RowData> values = createValue();
        return TestUpdateDeleteTableFactory.registerRowData(values);
    }

    private List<RowData> createValue() {
        List<RowData> values = new ArrayList<>();
        for (int i = 0; i < ROW_NUM; i++) {
            values.add(GenericRowData.of(i, StringData.fromString("b_" + i), i * 2.0));
        }
        return values;
    }

    private List<Row> toRows(TableResult result) {
        return CollectionUtil.iteratorToList(result.collect());
    }
}
