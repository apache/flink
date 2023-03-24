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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.factories.TableFactoryHarness;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;

/** Tests for {@link PushCalcPastChangelogNormalizeRule}. */
public class PushCalcPastChangelogNormalizeRuleTest extends TableTestBase {

    private StreamTableTestUtil util;

    @Before
    public void before() {
        util = streamTestUtil(TableConfig.getDefault());
    }

    @Test
    public void testWithSinglePrimaryKeyFilter() {
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", STRING())
                                        .column("f1", INT().notNull())
                                        .primaryKey("f1")
                                        .build())
                        .unboundedScanSource(ChangelogMode.upsert())
                        .build();

        util.tableEnv().createTable("T", sourceDescriptor);
        util.verifyRelPlan("SELECT * FROM T WHERE f1 < 1");
    }

    @Test
    public void testWithMultipleFilters() {
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", STRING())
                                        .column("f1", INT().notNull())
                                        .column("f2", STRING())
                                        .primaryKey("f1")
                                        .build())
                        .unboundedScanSource(ChangelogMode.upsert())
                        .build();

        util.tableEnv().createTable("T", sourceDescriptor);

        // Only the first filter (f1 < 10) can be pushed
        util.verifyRelPlan(
                "SELECT f1, SUM(f1) AS `sum` FROM T WHERE f1 < 10 AND (f1 > 3 OR f2 IS NULL) GROUP BY f1");
    }

    @Test
    public void testWithMultiplePrimaryKeyColumns() {
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", STRING())
                                        .column("f1", INT().notNull())
                                        .column("f2", BIGINT().notNull())
                                        .primaryKey("f1", "f2")
                                        .build())
                        .unboundedScanSource(ChangelogMode.upsert())
                        .build();

        util.tableEnv().createTable("T", sourceDescriptor);
        util.verifyRelPlan("SELECT f0, f1 FROM T WHERE (f1 < 1 OR f2 > 10) AND f0 IS NOT NULL");
    }

    @Test
    public void testOnlyProjection() {
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", STRING())
                                        .column("f1", INT().notNull())
                                        .column("f2", STRING().notNull())
                                        .primaryKey("f1")
                                        .build())
                        .unboundedScanSource(ChangelogMode.upsert())
                        .build();

        util.tableEnv().createTable("T", sourceDescriptor);
        util.verifyRelPlan("SELECT f1, f2 FROM T");
    }

    @Test
    public void testFilterAndProjection() {
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", STRING())
                                        .column("f1", INT().notNull())
                                        .column("f2", BIGINT().notNull())
                                        .column("f3", STRING())
                                        .column("f4", BIGINT().notNull())
                                        .column("f5", BIGINT().notNull())
                                        .column("f6", BIGINT().notNull())
                                        .column("f7", BIGINT().notNull())
                                        .primaryKey("f1", "f2")
                                        .build())
                        .unboundedScanSource(ChangelogMode.upsert())
                        .build();

        util.tableEnv().createTable("T", sourceDescriptor);
        util.verifyRelPlan("SELECT f1, f5 FROM T WHERE (f1 < 1 OR f2 > 10) AND f3 IS NOT NULL");
    }

    @Test
    public void testPartialPrimaryKeyFilterAndProjection() {
        final TableDescriptor sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", STRING())
                                        .column("f1", INT().notNull())
                                        .column("f2", BIGINT().notNull())
                                        .column("f3", STRING())
                                        .column("f4", BIGINT().notNull())
                                        .column("f5", BIGINT().notNull())
                                        .column("f6", BIGINT().notNull())
                                        .column("f7", BIGINT().notNull())
                                        .primaryKey("f1", "f2")
                                        .build())
                        .unboundedScanSource(ChangelogMode.upsert())
                        .build();

        util.tableEnv().createTable("T", sourceDescriptor);
        util.verifyRelPlan("SELECT f1, f5 FROM T WHERE f1 < 1 AND f3 IS NOT NULL");
    }
}
