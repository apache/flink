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

import static org.assertj.core.api.Assertions.assertThat;

/** The IT case for UPDATE clause in batch mode. */
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
        List<Row> rows =
                CollectionUtil.iteratorToList(tEnv().executeSql("select * from t").collect());
        assertThat(rows.toString())
                .isEqualTo("[+I[0, b_0, 0.0], +I[1, uaa, 4.0], +I[2, uaa, 16.0]]");
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
}
