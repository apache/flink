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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/**
 * Test for {@link PushLocalAggWithoutSortIntoTableSourceScanRule} and {@link
 * PushLocalAggWithSortIntoTableSourceScanRule}.
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
                        + "  id INT,\n"
                        + "  name STRING,\n"
                        + "  amount INT,\n"
                        + "  price DOUBLE,\n"
                        + "  type STRING\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'id',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);
    }

    @Test
    public void testCanPushDownWithGroup() {
        util.verifyRelPlan(
                "SELECT\n"
                        + "  sum(amount),\n"
                        + "  name,\n"
                        + "  type\n"
                        + "FROM inventory\n"
                        + "  group by name, type");
    }

    @Test
    public void testCanPushDownWithoutGroup() {
        util.verifyRelPlan(
                "SELECT\n"
                        + "  min(id),\n"
                        + "  max(amount),\n"
                        + "  max(name),\n"
                        + "  sum(price),\n"
                        + "  avg(price),\n"
                        + "  count(id)\n"
                        + "FROM inventory");
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
    public void testCannotPushDownWithFilter() {
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
