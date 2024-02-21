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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestUpdateDeleteTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The IT case for Truncate table statement in batch mode. */
public class TruncateTableITCase extends BatchTestBase {
    private static final int ROW_NUM = 2;

    @Test
    void testTruncateTable() {
        String dataId = registerData();
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE t (a int) WITH"
                                        + " ('connector' = 'test-update-delete',"
                                        + " 'data-id' = '%s',"
                                        + " 'only-accept-equal-predicate' = 'true'"
                                        + ")",
                                dataId));
        List<Row> rows = toRows(tEnv().executeSql("SELECT * FROM t"));
        assertThat(rows.toString()).isEqualTo("[+I[0], +I[1]]");
        tEnv().executeSql("TRUNCATE TABLE t");
        rows = toRows(tEnv().executeSql("SELECT * FROM t"));
        assertThat(rows).isEmpty();
    }

    @Test
    void testTruncateTableWithoutImplementation() {
        tEnv().executeSql("CREATE TABLE t (a int) WITH" + " ('connector' = 'values')");
        assertThatThrownBy(() -> tEnv().executeSql("TRUNCATE TABLE t"))
                .isInstanceOf(TableException.class)
                .hasMessage(
                        "TRUNCATE TABLE statement is not supported for the table default_catalog.default_database.t"
                                + " since the table hasn't implemented the interface"
                                + " org.apache.flink.table.connector.sink.abilities.SupportsTruncate.");
    }

    @Test
    void testTruncateLegacyTable() {
        tEnv().executeSql(
                        "CREATE TABLE t (a int, b string, c double) WITH"
                                + " ('connector' = 'COLLECTION')");
        assertThatThrownBy(() -> tEnv().executeSql("TRUNCATE TABLE t"))
                .isInstanceOf(TableException.class)
                .hasMessage(
                        String.format(
                                "Can't perform truncate table operation of the table %s "
                                        + "because the corresponding table sink is the legacy "
                                        + "TableSink."
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
            values.add(GenericRowData.of(i));
        }
        return values;
    }

    private List<Row> toRows(TableResult result) {
        return CollectionUtil.iteratorToList(result.collect());
    }
}
