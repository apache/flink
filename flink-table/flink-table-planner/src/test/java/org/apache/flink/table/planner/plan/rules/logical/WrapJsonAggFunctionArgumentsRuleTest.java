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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;

/** Tests for {@link WrapJsonAggFunctionArgumentsRule}. */
public class WrapJsonAggFunctionArgumentsRuleTest extends TableTestBase {

    private StreamTableTestUtil util;

    @Before
    public void before() {
        util = streamTestUtil(TableConfig.getDefault());
    }

    @Test
    public void testJsonObjectAgg() {
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(Schema.newBuilder().column("f0", STRING()).build())
                        .unboundedScanSource(ChangelogMode.all())
                        .build();

        util.tableEnv().createTable("T", sourceDescriptor);
        util.verifyRelPlan("SELECT JSON_OBJECTAGG(f0 VALUE f0) FROM T");
    }

    @Test
    public void testJsonObjectAggInGroupWindow() {
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", INT())
                                        .column("f1", STRING())
                                        .build())
                        .unboundedScanSource()
                        .build();

        util.tableEnv().createTable("T", sourceDescriptor);
        util.verifyRelPlan("SELECT f0, JSON_OBJECTAGG(f1 VALUE f0) FROM T GROUP BY f0");
    }

    @Test
    public void testJsonArrayAgg() {
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(Schema.newBuilder().column("f0", STRING()).build())
                        .unboundedScanSource(ChangelogMode.all())
                        .build();

        util.tableEnv().createTable("T", sourceDescriptor);
        util.verifyRelPlan("SELECT JSON_ARRAYAGG(f0) FROM T");
    }

    @Test
    public void testJsonArrayAggInGroupWindow() {
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(Schema.newBuilder().column("f0", INT()).build())
                        .unboundedScanSource()
                        .build();

        util.tableEnv().createTable("T", sourceDescriptor);
        util.verifyRelPlan("SELECT f0, JSON_ARRAYAGG(f0) FROM T GROUP BY f0");
    }
}
