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

package org.apache.flink.table.api.internal;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link StatementSetImpl}. */
public class StatementSetImplTest {

    TableEnvironmentInternal tableEnv;

    @Before
    public void setup() {
        tableEnv =
                (TableEnvironmentInternal)
                        TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    @Test
    public void testGetJsonPlan() throws IOException {
        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tableEnv.executeSql(srcTableDdl);

        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tableEnv.executeSql(sinkTableDdl);

        StatementSet stmtSet = tableEnv.createStatementSet();
        stmtSet.addInsertSql("INSERT INTO MySink SELECT * FROM MyTable");
        String jsonPlan = stmtSet.compilePlan().asJsonString();
        String actual = TableTestUtil.readFromResource("/jsonplan/testGetJsonPlan.out");
        assertThat(
                        TableTestUtil.getFormattedJson(
                                TableTestUtil.replaceExecNodeId(
                                        TableTestUtil.getFormattedJson(actual))))
                .isEqualTo(
                        TableTestUtil.getFormattedJson(
                                TableTestUtil.replaceExecNodeId(
                                        TableTestUtil.replaceFlinkVersion(
                                                TableTestUtil.getFormattedJson(jsonPlan)))));
    }
}
