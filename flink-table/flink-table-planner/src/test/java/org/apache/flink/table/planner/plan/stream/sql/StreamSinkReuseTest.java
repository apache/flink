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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.common.SinkReuseTestBase;
import org.apache.flink.table.planner.plan.reuse.SinkReuser;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link SinkReuser} in streaming mode. */
public class StreamSinkReuseTest extends SinkReuseTestBase {
    @Override
    protected TableTestUtil getTableTestUtil(TableConfig tableConfig) {
        return streamTestUtil(tableConfig);
    }

    @Override
    @BeforeEach
    protected void setup() {
        super.setup();
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE retractSource (\n"
                                + "  x INT,\n"
                                + "  y INT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'changelog-mode' = 'I, UA, UB, D',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE upsertSource (\n"
                                + "  x INT,\n"
                                + "  y INT,\n"
                                + "  PRIMARY KEY (`x`) NOT ENFORCED \n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'changelog-mode' = 'I, UA, D',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE upsertSource2 (\n"
                                + "  x INT,\n"
                                + "  y INT,\n"
                                + "  PRIMARY KEY (`y`) NOT ENFORCED \n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'changelog-mode' = 'I, UA, D',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE upsertSink (\n"
                                + "  a BIGINT,\n"
                                + "  b BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values', \n"
                                + " 'sink-insert-only' = 'false' \n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE upsertPKSink (\n"
                                + "  a BIGINT,\n"
                                + "  b BIGINT,\n"
                                + "  PRIMARY KEY (`b`) NOT ENFORCED \n"
                                + ") WITH (\n"
                                + " 'connector' = 'values', \n"
                                + " 'sink-insert-only' = 'false' \n"
                                + ")");
    }

    @Test
    public void testSinkReuseWithUpsertAndRetract() {
        StatementSet statementSet = util.tableEnv().createStatementSet();
        statementSet.addInsertSql("INSERT INTO upsertSink (SELECT * FROM retractSource)");
        statementSet.addInsertSql("INSERT INTO upsertSink (SELECT * FROM source1)");
        statementSet.addInsertSql("INSERT INTO upsertSink (SELECT * FROM upsertSource)");
        util.verifyExecPlan(statementSet);
    }

    @Test
    public void testSinkReuseWithUpsertMaterialize() {
        StatementSet statementSet = util.tableEnv().createStatementSet();
        statementSet.addInsertSql("INSERT INTO upsertPKSink (SELECT * FROM upsertSource)");
        statementSet.addInsertSql("INSERT INTO upsertPKSink (SELECT * FROM upsertSource)");
        statementSet.addInsertSql("INSERT INTO upsertPKSink (SELECT * FROM upsertSource2)");
        util.verifyExecPlan(statementSet);
    }
}
