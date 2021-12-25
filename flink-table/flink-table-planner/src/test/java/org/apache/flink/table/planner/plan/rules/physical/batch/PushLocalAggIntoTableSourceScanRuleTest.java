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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.functions.aggfunctions.CollectAggFunction;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/**
 * Test for rules that extend {@link PushLocalAggIntoScanRuleBase} to push down local aggregates
 * into table source.
 */
public class PushLocalAggIntoTableSourceScanRuleTest extends TableTestBase {
    protected BatchTableTestUtil util = batchTestUtil(new TableConfig());

    @Before
    public void setup() {
        TableConfig tableConfig = util.tableEnv().getConfig();
        tableConfig
                .getConfiguration()
                .setBoolean(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED,
                        true);
        String ddl =
                "CREATE TABLE inventory (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT,\n"
                        + "  type STRING\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'id;type',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);

        String ddl2 =
                "CREATE TABLE inventory_meta (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT,\n"
                        + "  type STRING,\n"
                        + "  metadata_1 BIGINT METADATA,\n"
                        + "  metadata_2 STRING METADATA,\n"
                        + "  PRIMARY KEY (`id`) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'id;type',\n"
                        + " 'readable-metadata' = 'metadata_1:BIGINT, metadata_2:STRING',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl2);

        // partitioned table
        String ddl3 =
                "CREATE TABLE inventory_part (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT,\n"
                        + "  type STRING\n"
                        + ") PARTITIONED BY (type)\n"
                        + "WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'id;type',\n"
                        + " 'partition-list' = 'type:a;type:b',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl3);

        // disable projection push down
        String ddl4 =
                "CREATE TABLE inventory_no_proj (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  amount BIGINT,\n"
                        + "  price BIGINT,\n"
                        + "  type STRING\n"
                        + ")\n"
                        + "WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'id;type',\n"
                        + " 'enable-projection-push-down' = 'false',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl4);
    }

    @Test
    public void testCanPushDownLocalHashAggWithGroup() {
        util.verifyRelPlan(
                "SELECT\n"
                        + "  sum(amount),\n"
                        + "  name,\n"
                        + "  type\n"
                        + "FROM inventory\n"
                        + "  group by name, type");
    }

    @Test
    public void testDisablePushDownLocalAgg() {
        // disable push down local agg
        util.getTableEnv()
                .getConfig()
                .getConfiguration()
                .setBoolean(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED,
                        false);

        util.verifyRelPlan(
                "SELECT\n"
                        + "  sum(amount),\n"
                        + "  name,\n"
                        + "  type\n"
                        + "FROM inventory\n"
                        + "  group by name, type");

        // reset config
        util.getTableEnv()
                .getConfig()
                .getConfiguration()
                .setBoolean(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED,
                        true);
    }

    @Test
    public void testCanPushDownLocalHashAggWithoutGroup() {
        util.verifyRelPlan(
                "SELECT\n"
                        + "  min(id),\n"
                        + "  max(amount),\n"
                        + "  sum(price),\n"
                        + "  avg(price),\n"
                        + "  count(id)\n"
                        + "FROM inventory");
    }

    @Test
    public void testCanPushDownLocalSortAggWithoutSort() {
        // enable sort agg
        util.getTableEnv()
                .getConfig()
                .getConfiguration()
                .setString(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg");

        util.verifyRelPlan(
                "SELECT\n"
                        + "  min(id),\n"
                        + "  max(amount),\n"
                        + "  sum(price),\n"
                        + "  avg(price),\n"
                        + "  count(id)\n"
                        + "FROM inventory");

        // reset config
        util.getTableEnv()
                .getConfig()
                .getConfiguration()
                .setString(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "");
    }

    @Test
    public void testCanPushDownLocalSortAggWithSort() {
        // enable sort agg
        util.getTableEnv()
                .getConfig()
                .getConfiguration()
                .setString(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg");

        util.verifyRelPlan(
                "SELECT\n"
                        + "  sum(amount),\n"
                        + "  name,\n"
                        + "  type\n"
                        + "FROM inventory\n"
                        + "  group by name, type");

        // reset config
        util.getTableEnv()
                .getConfig()
                .getConfiguration()
                .setString(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "");
    }

    @Test
    public void testCanPushDownLocalAggAfterFilterPushDown() {

        util.verifyRelPlan(
                "SELECT\n"
                        + "  sum(amount),\n"
                        + "  name,\n"
                        + "  type\n"
                        + "FROM inventory\n"
                        + "  where id = 123\n"
                        + "  group by name, type");
    }

    @Test
    public void testCanPushDownLocalAggWithMetadata() {
        util.verifyRelPlan(
                "SELECT\n"
                        + "  sum(amount),\n"
                        + "  max(metadata_1),\n"
                        + "  name,\n"
                        + "  type\n"
                        + "FROM inventory_meta\n"
                        + "  where id = 123\n"
                        + "  group by name, type");
    }

    @Test
    public void testCanPushDownLocalAggWithPartition() {
        util.verifyRelPlan(
                "SELECT\n"
                        + "  sum(amount),\n"
                        + "  type,\n"
                        + "  name\n"
                        + "FROM inventory_part\n"
                        + "  where type in ('a', 'b') and id = 123\n"
                        + "  group by type, name");
    }

    @Test
    public void testCanPushDownLocalAggWithoutProjectionPushDown() {
        util.verifyRelPlan(
                "SELECT\n"
                        + "  sum(amount),\n"
                        + "  name,\n"
                        + "  type\n"
                        + "FROM inventory_no_proj\n"
                        + "  where id = 123\n"
                        + "  group by name, type");
    }

    @Test
    public void testCanPushDownLocalAggWithAuxGrouping() {
        // enable two-phase aggregate, otherwise there is no local aggregate
        util.getTableEnv()
                .getConfig()
                .getConfiguration()
                .setString(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE");

        util.verifyRelPlan(
                "SELECT\n"
                        + "  id, name, count(*)\n"
                        + "FROM inventory_meta\n"
                        + "  group by id, name");
    }

    @Test
    public void testCannotPushDownLocalAggAfterLimitPushDown() {

        util.verifyRelPlan(
                "SELECT\n"
                        + "  sum(amount),\n"
                        + "  name,\n"
                        + "  type\n"
                        + "FROM (\n"
                        + "  SELECT\n"
                        + "    *\n"
                        + "  FROM inventory\n"
                        + "  LIMIT 100\n"
                        + ") t\n"
                        + "  group by name, type");
    }

    @Test
    public void testCannotPushDownLocalAggWithUDAF() {
        // add udf
        util.addTemporarySystemFunction(
                "udaf_collect", new CollectAggFunction<>(DataTypes.BIGINT().getLogicalType()));

        util.verifyRelPlan(
                "SELECT\n"
                        + "  udaf_collect(amount),\n"
                        + "  name,\n"
                        + "  type\n"
                        + "FROM inventory\n"
                        + "  group by name, type");
    }

    @Test
    public void testCannotPushDownLocalAggWithUnsupportedDataTypes() {
        util.verifyRelPlan(
                "SELECT\n"
                        + "  max(name),\n"
                        + "  type\n"
                        + "FROM inventory\n"
                        + "  group by type");
    }

    @Test
    public void testCannotPushDownWithColumnExpression() {
        util.verifyRelPlan(
                "SELECT\n"
                        + "  min(amount + price),\n"
                        + "  max(amount),\n"
                        + "  sum(price),\n"
                        + "  count(id),\n"
                        + "  name\n"
                        + "FROM inventory\n"
                        + "  group by name");
    }

    @Test
    public void testCannotPushDownWithUnsupportedAggFunction() {
        util.verifyRelPlan(
                "SELECT\n"
                        + "  min(id),\n"
                        + "  max(amount),\n"
                        + "  sum(price),\n"
                        + "  count(distinct id),\n"
                        + "  name\n"
                        + "FROM inventory\n"
                        + "  group by name");
    }

    @Test
    public void testCannotPushDownWithWindowAggFunction() {
        util.verifyRelPlan(
                "SELECT\n"
                        + "  id,\n"
                        + "  amount,\n"
                        + "  sum(price) over (partition by name),\n"
                        + "  name\n"
                        + "FROM inventory");
    }

    @Test
    public void testCannotPushDownWithArgFilter() {
        util.verifyRelPlan(
                "SELECT\n"
                        + "  min(id),\n"
                        + "  max(amount),\n"
                        + "  sum(price),\n"
                        + "  count(id) FILTER(WHERE id > 100),\n"
                        + "  name\n"
                        + "FROM inventory\n"
                        + "  group by name");
    }
}
