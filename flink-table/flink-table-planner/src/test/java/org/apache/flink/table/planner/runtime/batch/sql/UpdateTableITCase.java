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
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The IT case for UPDATE statement in batch mode. */
@RunWith(Parameterized.class)
public class UpdateTableITCase extends BatchTestBase {
    private final SupportsRowLevelUpdate.RowLevelUpdateMode updateMode;

    @Parameterized.Parameters(name = "updateMode = {0}")
    public static Collection<SupportsRowLevelUpdate.RowLevelUpdateMode> data() {
        return Arrays.asList(
                SupportsRowLevelUpdate.RowLevelUpdateMode.UPDATED_ROWS,
                SupportsRowLevelUpdate.RowLevelUpdateMode.ALL_ROWS);
    }

    public UpdateTableITCase(SupportsRowLevelUpdate.RowLevelUpdateMode updateMode) {
        this.updateMode = updateMode;
    }

    @Test
    public void testUpdate() throws Exception {
        String dataId = registerData();
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE t ("
                                        + " a int PRIMARY KEY NOT ENFORCED,"
                                        + " b string,"
                                        + " c double) WITH"
                                        + " ('connector' = 'test-update-delete', "
                                        + "'data-id' = '%s', "
                                        + "'update-mode' = '%s')",
                                dataId, updateMode));
        tEnv().executeSql("UPDATE t SET b = 'uaa', c = c * c WHERE a >= 1").await();
        List<String> rows = toSortedResults(tEnv().executeSql("SELECT * FROM t"));
        assertThat(rows.toString())
                .isEqualTo("[+I[0, b_0, 0.0], +I[1, uaa, 4.0], +I[2, uaa, 16.0]]");

        tEnv().executeSql("UPDATE t SET b = 'uab' WHERE a > (SELECT count(1) FROM t WHERE a > 1)")
                .await();
        rows = toSortedResults(tEnv().executeSql("SELECT * FROM t"));
        assertThat(rows.toString())
                .isEqualTo("[+I[0, b_0, 0.0], +I[1, uaa, 4.0], +I[2, uab, 16.0]]");
    }

    @Test
    public void testPartialUpdate() throws Exception {
        String dataId = registerData();
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE t ("
                                        + " a int PRIMARY KEY NOT ENFORCED,"
                                        + " b string not null,"
                                        + " c double not null) WITH"
                                        + " ('connector' = 'test-update-delete', "
                                        + "'data-id' = '%s',"
                                        + " 'required-columns-for-update' = 'a;b', "
                                        + " 'update-mode' = '%s')",
                                dataId, updateMode));
        tEnv().executeSql("UPDATE t SET b = 'uaa' WHERE a >= 1").await();
        List<String> rows = toSortedResults(tEnv().executeSql("SELECT * FROM t"));
        assertThat(rows.toString())
                .isEqualTo("[+I[0, b_0, 0.0], +I[1, uaa, 2.0], +I[2, uaa, 4.0]]");

        // test partial update with requiring partial primary keys
        dataId = registerData();
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE t1 ("
                                        + " a int,"
                                        + " b string not null,"
                                        + " c double not null,"
                                        + " PRIMARY KEY (a, c) NOT ENFORCED"
                                        + ") WITH"
                                        + " ('connector' = 'test-update-delete', "
                                        + "'data-id' = '%s',"
                                        + " 'required-columns-for-update' = 'a;b', "
                                        + " 'update-mode' = '%s')",
                                dataId, updateMode));
        tEnv().executeSql("UPDATE t1 SET b = 'uaa' WHERE a >= 1").await();
        rows = toSortedResults(tEnv().executeSql("SELECT * FROM t1"));
        assertThat(rows.toString())
                .isEqualTo("[+I[0, b_0, 0.0], +I[1, uaa, 2.0], +I[2, uaa, 4.0]]");
    }

    @Test
    public void testStatementSetContainUpdateAndInsert() throws Exception {
        tEnv().executeSql(
                        "CREATE TABLE t (a int, b string, c double) WITH"
                                + " ('connector' = 'test-update-delete')");
        StatementSet statementSet = tEnv().createStatementSet();
        // should throw exception when statement set contains insert and update statement
        statementSet.addInsertSql("INSERT INTO t VALUES (1, 'v1', 1)");
        statementSet.addInsertSql("UPDATE t SET b = 'uaa'");
        assertThatThrownBy(statementSet::execute)
                .isInstanceOf(TableException.class)
                .hasMessage(
                        "Unsupported SQL query! Only accept a single SQL statement of type UPDATE.");
    }

    @Test
    public void testCompilePlanSql() throws Exception {
        tEnv().executeSql(
                        "CREATE TABLE t (a int, b string, c double) WITH"
                                + " ('connector' = 'test-update-delete')");
        // should throw exception when compile sql for update statement
        assertThatThrownBy(() -> tEnv().compilePlanSql("UPDATE t SET b = 'uaa'"))
                .isInstanceOf(TableException.class)
                .hasMessage(
                        "Unsupported SQL query! compilePlanSql() only accepts a single SQL statement of type INSERT");
    }

    @Test
    public void testUpdateWithLegacyTableSink() {
        tEnv().executeSql(
                        "CREATE TABLE t (a int, b string, c double) WITH"
                                + " ('connector' = 'COLLECTION')");
        assertThatThrownBy(() -> tEnv().executeSql("UPDATE t SET b = 'uaa'"))
                .isInstanceOf(TableException.class)
                .hasMessage(
                        String.format(
                                "Can't perform update operation of the table %s "
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
        for (int i = 0; i < 3; i++) {
            values.add(GenericRowData.of(i, StringData.fromString("b_" + i), i * 2.0));
        }
        return values;
    }

    private List<String> toSortedResults(TableResult result) {
        return CollectionUtil.iteratorToList(result.collect()).stream()
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());
    }
}
