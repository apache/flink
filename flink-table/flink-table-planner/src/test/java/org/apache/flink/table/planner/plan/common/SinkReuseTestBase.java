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

package org.apache.flink.table.planner.plan.common;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.reuse.SinkReuser;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Base test class for {@link SinkReuser}. */
public abstract class SinkReuseTestBase extends TableTestBase {
    protected TableTestUtil util;

    @BeforeEach
    protected void setup() {
        TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true);
        tableConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SINK_ENABLED, true);
        util = getTableTestUtil(tableConfig);

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sink1 (\n"
                                + "  x BIGINT,\n"
                                + "  y BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'filesystem',\n"
                                + " 'format' = 'test-format',\n"
                                + " 'test-format.delimiter' = ',',\n"
                                + "'path' = 'ignore'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sink2 (\n"
                                + "  x BIGINT,\n"
                                + "  y BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE partitionedSink (\n"
                                + "  a BIGINT,\n"
                                + "  b BIGINT\n"
                                + ")  PARTITIONED BY(`a`) WITH (\n"
                                + " 'connector' = 'values'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE source1 (\n"
                                + "  x BIGINT,\n"
                                + "  y BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE source2 (\n"
                                + "  x BIGINT,\n"
                                + "  y BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE source3 (\n"
                                + "  x BIGINT,\n"
                                + "  y BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE source4 (\n"
                                + "  x BIGINT,\n"
                                + "  y BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE filed_name_change_source (\n"
                                + "  x1 BIGINT,\n"
                                + "  y1 BIGINT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE type_coercion_source (\n"
                                + "  x INT,\n"
                                + "  y INT\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'disable-lookup' = 'true',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
    }

    @Test
    public void testSinkReuse() {
        StatementSet statementSet = util.tableEnv().createStatementSet();
        statementSet.addInsertSql("INSERT INTO sink1 (SELECT * FROM source1)");
        statementSet.addInsertSql("INSERT INTO sink1 (SELECT * FROM source2)");
        statementSet.addInsertSql("INSERT INTO sink2 (SELECT * FROM source3)");
        statementSet.addInsertSql("INSERT INTO sink2 (SELECT * FROM source4)");
        util.verifyExecPlan(statementSet);
    }

    @Test
    public void testSinkReuseFromSameSource() {
        StatementSet statementSet = util.tableEnv().createStatementSet();
        statementSet.addInsertSql("INSERT INTO sink1 (SELECT * FROM source1)");
        statementSet.addInsertSql("INSERT INTO sink1 (SELECT * FROM source1)");
        util.verifyExecPlan(statementSet);
    }

    @Test
    public void testSinkReuseWithPartialColumns() {
        StatementSet statementSet = util.tableEnv().createStatementSet();
        statementSet.addInsertSql("INSERT INTO sink1(`x`) (SELECT x FROM source1)");
        statementSet.addInsertSql("INSERT INTO sink1(`y`) (SELECT y FROM source1)");
        statementSet.addInsertSql("INSERT INTO sink1(`x`) (SELECT x FROM source3)");
        util.verifyExecPlan(statementSet);
    }

    @Test
    public void testSinkReuseWithOverwrite() {
        StatementSet statementSet = util.tableEnv().createStatementSet();
        statementSet.addInsertSql("INSERT OVERWRITE sink1 (SELECT * FROM source1)");
        statementSet.addInsertSql("INSERT INTO sink1 (SELECT * FROM source2)");
        statementSet.addInsertSql("INSERT OVERWRITE sink1 (SELECT * FROM source3)");
        util.verifyExecPlan(statementSet);
    }

    @Test
    public void testSinkReuseWithPartition() {
        StatementSet statementSet = util.tableEnv().createStatementSet();
        statementSet.addInsertSql(
                "INSERT INTO partitionedSink PARTITION(a = 1) (SELECT y FROM source1)");
        statementSet.addInsertSql(
                "INSERT INTO partitionedSink PARTITION(a = 1) (SELECT y FROM source2)");
        statementSet.addInsertSql(
                "INSERT INTO partitionedSink PARTITION(a = 2) (SELECT y FROM source3)");
        statementSet.addInsertSql("INSERT INTO partitionedSink (SELECT * FROM source4)");
        util.verifyExecPlan(statementSet);
    }

    @Test
    public void testSinkReuseWithDifferentFieldNames() {
        StatementSet statementSet = util.tableEnv().createStatementSet();
        statementSet.addInsertSql("INSERT INTO sink1 (SELECT x, y FROM source1)");
        statementSet.addInsertSql(
                "INSERT INTO sink1 (SELECT x1, y1 FROM filed_name_change_source)");
        util.verifyExecPlan(statementSet);
    }

    @Test
    public void testSinkReuseWithHint() {
        StatementSet statementSet = util.tableEnv().createStatementSet();
        statementSet.addInsertSql(
                "INSERT INTO sink1 /*+ OPTIONS('path' = 'ignore1') */ (SELECT * FROM source1)");
        statementSet.addInsertSql(
                "INSERT INTO sink1 /*+ OPTIONS('path' = 'ignore2') */ (SELECT * FROM source2)");
        statementSet.addInsertSql(
                "INSERT INTO sink1 /*+ OPTIONS('path' = 'ignore1') */ (SELECT * FROM source3)");
        util.verifyExecPlan(statementSet);
    }

    @Test
    public void testSinkReuseWithTypeCoercionSource() {
        StatementSet statementSet = util.tableEnv().createStatementSet();
        statementSet.addInsertSql("INSERT INTO sink1 (SELECT * FROM source1)");
        statementSet.addInsertSql("INSERT INTO sink1 (SELECT * FROM type_coercion_source)");
        util.verifyExecPlan(statementSet);
    }

    protected abstract TableTestUtil getTableTestUtil(TableConfig tableConfig);
}
