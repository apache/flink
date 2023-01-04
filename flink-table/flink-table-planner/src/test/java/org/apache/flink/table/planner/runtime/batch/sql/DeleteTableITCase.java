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

/** The IT case for DELETE clause. */
@RunWith(Parameterized.class)
public class DeleteTableITCase extends BatchTestBase {

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
    public void testDelete() throws Exception {
        String dataId = registerData();
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE t (a int, b string, c double) WITH"
                                        + " ('connector' = 'test-update-delete',"
                                        + " 'data-id' = '%s',"
                                        + " 'delete-mode' = '%s'"
                                        + ")",
                                dataId, deleteMode));
        tEnv().executeSql("DELETE FROM t WHERE a > 1").await();
        List<Row> rows =
                CollectionUtil.iteratorToList(tEnv().executeSql("SELECT * FROM t").collect());
        assertThat(rows.toString()).isEqualTo("[+I[0, b_0, 0.0], +I[1, b_1, 2.0]]");
    }

    @Test
    public void testDeleteMixWithPushDown() throws Exception {
        String dataId = registerData();
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE t1 (a int, b string, c double) WITH"
                                        + " ('connector' = 'test-update-delete', "
                                        + "'data-id' = '%s', "
                                        + "'support-delete-push-down' = 'true', "
                                        + "'only-accept-empty-filter' = 'true', "
                                        + "'delete-mode' = '%s'"
                                        + ")",
                                dataId, deleteMode));

        List<Row> rows =
                CollectionUtil.iteratorToList(tEnv().executeSql("DELETE FROM t1").collect());
        assertThat(rows.toString()).isEqualTo("[+I[OK]]");

        rows = CollectionUtil.iteratorToList(tEnv().executeSql("select * from t1").collect());
        assertThat(rows.toString()).isEqualTo("[]");

        // should fall back to row level delete
        dataId = registerData();
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE t2 (a int, b string, c double) WITH"
                                        + " ('connector' = 'test-update-delete', "
                                        + "'data-id' = '%s', "
                                        + "'support-delete-push-down' = 'true', "
                                        + "'only-accept-empty-filter' = 'true', "
                                        + "'delete-mode' = '%s'"
                                        + ")",
                                dataId, deleteMode));

        tEnv().executeSql("DELETE FROM t2 WHERE a > 1").await();
        rows = CollectionUtil.iteratorToList(tEnv().executeSql("select * from t2").collect());
        assertThat(rows.toString()).isEqualTo("[+I[0, b_0, 0.0], +I[1, b_1, 2.0]]");
    }

    private String registerData() {
        List<RowData> values = createValue();
        return TestUpdateDeleteTableFactory.registerRowData(values);
    }

    private List<RowData> createValue() {
        List<RowData> values = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            values.add(GenericRowData.of(i, StringData.fromString("b_" + i), i * 2.0));
        }
        return values;
    }
}
