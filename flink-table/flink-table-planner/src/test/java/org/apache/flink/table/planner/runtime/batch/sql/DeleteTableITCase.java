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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.factories.TestUpdateDeleteTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The IT case for DELETE statement in batch mode. */
public class DeleteTableITCase extends BatchTestBase {
    private static final int ROW_NUM = 5;

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
        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql("DELETE FROM t WHERE a = 1").collect());
        assertThat(rows.toString()).isEqualTo("[+I[1], +I[OK]]");
        rows = CollectionUtil.iteratorToList(tEnv().executeSql("SELECT * FROM t").collect());
        assertThat(rows.toString())
                .isEqualTo("[+I[0, b_0, 0.0], +I[2, b_2, 4.0], +I[3, b_3, 6.0], +I[4, b_4, 8.0]]");

        // should throw exception for the deletion can not be pushed down as it contains non-equal
        // predicate and the table sink haven't implemented SupportsRowLevelDeleteInterface
        assertThatThrownBy(() -> tEnv().executeSql("DELETE FROM t where a > 1"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Only delete push down is supported currently, "
                                + "but the delete statement can't pushed to table sink `default_catalog`.`default_database`.`t`.");

        tEnv().executeSql(
                        "CREATE TABLE t1 (a int) WITH"
                                + " ('connector' = 'test-update-delete',"
                                + " 'support-delete-push-down' = 'false')");
        // should throw exception for sink that doesn't implement SupportsDeletePushDown interface
        assertThatThrownBy(() -> tEnv().executeSql("DELETE FROM t1"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Only delete push down is supported currently, "
                                + "but the delete statement can't pushed to table sink `default_catalog`.`default_database`.`t1`.");
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
}
