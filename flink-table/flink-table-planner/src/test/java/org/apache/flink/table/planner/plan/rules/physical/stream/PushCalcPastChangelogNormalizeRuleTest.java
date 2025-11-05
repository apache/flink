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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;

/** Tests for {@link PushCalcPastChangelogNormalizeRule}. */
class PushCalcPastChangelogNormalizeRuleTest extends TableTestBase {

    private StreamTableTestUtil util;
    private static TableDescriptor sourceDescriptor;
    private static TableDescriptor sourceDescriptorWithTwoPrimaryKeys;

    @BeforeAll
    static void setup() {
        sourceDescriptor =
                TableFactoryHarness.newBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("f0", INT())
                                        .column("f1", INT().notNull())
                                        .column("f2", STRING())
                                        .column("f3", BIGINT().notNull())
                                        .primaryKey("f1")
                                        .build())
                        .unboundedScanSource(ChangelogMode.upsert())
                        .build();

        sourceDescriptorWithTwoPrimaryKeys =
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
    }

    @BeforeEach
    void before() {
        util = streamTestUtil(TableConfig.getDefault());
    }

    @Test
    void testWithSinglePrimaryKeyFilter() {
        util.tableEnv().createTable("T", sourceDescriptor);
        util.verifyRelPlan("SELECT * FROM T WHERE f1 < 1");
    }

    @Test
    void testPrimaryKeySeveralSameSources() {
        util.tableEnv().createTable("T", sourceDescriptor);
        // Shouldn't be pushed down since there is no common filter for ChangelogNormalize with the
        // same source
        // verifyExecPlan is intended here as it will show whether the node is reused or not
        util.verifyExecPlan(
                "SELECT * FROM T WHERE f1 < 1\n"
                        + "UNION SELECT * FROM T WHERE f1 < 3\n"
                        + "INTERSECT SELECT * FROM T WHERE f1 > 0");
    }

    @Test
    void testPrimaryKeySeveralSameSourcesWithPartialPushDown() {
        util.tableEnv().createTable("T", sourceDescriptor);
        // Here filter should be partially pushed down
        // verifyExecPlan is intended here as it will show whether the node is reused or not
        util.verifyExecPlan(
                "SELECT * FROM T WHERE f1 < 1 AND f1 > 0\n"
                        + " UNION SELECT * FROM T WHERE f1 < 3 AND f1 > 0\n"
                        + " INTERSECT SELECT * FROM T WHERE f1 > 0 AND f1 < 10");
    }

    @Test
    void testPrimaryKeySeveralSameSourcesWithFullPushDown() {
        util.tableEnv().createTable("T", sourceDescriptor);
        // Here filter should be fully pushed down
        // verifyExecPlan is intended here as it will show whether the node is reused or not
        util.verifyExecPlan(
                "SELECT * FROM T WHERE f1 < 1 AND f1 > 0\n"
                        + " UNION SELECT * FROM T WHERE f1 < 1 AND f1 > 0\n"
                        + " INTERSECT SELECT * FROM T WHERE f1 < 1 AND f1 > 0");
    }

    @Test
    void testPrimaryKeySeveralDifferentSources() {
        util.tableEnv().createTable("T", sourceDescriptor);
        util.tableEnv().createTable("T2", sourceDescriptor);
        util.tableEnv().createTable("T3", sourceDescriptor);
        // verifyExecPlan is intended here as it will show whether the node is reused or not
        util.verifyExecPlan(
                "SELECT * FROM T WHERE f1 < 1 AND f1 > 0\n"
                        + " UNION SELECT * FROM T2 WHERE f1 < 1 AND f1 > 0\n"
                        + " INTERSECT SELECT * FROM T3 WHERE f1 < 1 AND f1 > 0");
    }

    @Test
    void testNonPrimaryKeySeveralSameSources() {
        util.tableEnv().createTable("T", sourceDescriptor);
        // verifyExecPlan is intended here as it will show whether the node is reused or not
        util.verifyExecPlan(
                "SELECT * FROM T WHERE f3 < 1\n"
                        + "UNION SELECT * FROM T WHERE f3 < 3\n"
                        + "INTERSECT SELECT * FROM T WHERE f3 > 0");
    }

    @Test
    void testNonPrimaryKeySeveralSameSourcesPartialPushedDown() {
        util.tableEnv().createTable("T", sourceDescriptor);
        // verifyExecPlan is intended here as it will show whether the node is reused or not
        util.verifyExecPlan(
                "SELECT * FROM T WHERE f3 < 1 AND f3 > 0\n"
                        + " UNION SELECT * FROM T WHERE f3 < 3 AND f3 > 0\n"
                        + " INTERSECT SELECT * FROM T WHERE f3 > 0 AND f3 < 10");
    }

    @Test
    void testNonPrimaryKeySeveralSameSourcesWithFullPushDown() {
        util.tableEnv().createTable("T", sourceDescriptor);
        // Here filter should be fully pushed down
        // verifyExecPlan is intended here as it will show whether the node is reused or not
        util.verifyExecPlan(
                "SELECT * FROM T WHERE f3 < 1 AND f3 > 0\n"
                        + " UNION SELECT * FROM T WHERE f3 < 1 AND f3 > 0\n"
                        + " INTERSECT SELECT * FROM T WHERE f3 < 1 AND f3 > 0");
    }

    @Test
    void testNonPrimaryKeySeveralDifferentSources() {
        util.tableEnv().createTable("T", sourceDescriptor);
        util.tableEnv().createTable("T2", sourceDescriptor);
        util.tableEnv().createTable("T3", sourceDescriptor);
        // verifyExecPlan is intended here as it will show whether the node is reused or not
        util.verifyExecPlan(
                "SELECT * FROM T WHERE f3 < 1 AND f3 > 0\n"
                        + " UNION SELECT * FROM T2 WHERE f3 < 1 AND f3 > 0\n"
                        + " INTERSECT SELECT * FROM T3 WHERE f3 < 1 AND f3 > 0");
    }

    @Test
    void testNonPrimaryKeySameSourcesAndSargNotPushedDown() {
        util.tableEnv().createTable("T", sourceDescriptor);
        // Here IS NOT NULL filter should be pushed down, SARG should stay in Calc
        // verifyExecPlan is intended here as it will show whether the node is reused or not
        util.verifyExecPlan(
                "SELECT * FROM T WHERE f0 < 10 AND f0 > 1 AND f0 IS NOT NULL\n"
                        + " UNION SELECT * FROM T WHERE f0 < 2 AND f0 > 0 AND f0 IS NOT NULL\n"
                        + " INTERSECT SELECT * FROM T WHERE f0 < 4 AND f0 > 2 AND f0 IS NOT NULL");
    }

    @Test
    void testWithMultipleFilters() {
        util.tableEnv().createTable("T", sourceDescriptor);

        // Only the first filter (f1 < 10) can be pushed
        util.verifyRelPlan(
                "SELECT f1, SUM(f1) AS `sum` FROM T WHERE f1 < 10 AND (f1 > 3 OR f2 IS NULL) GROUP BY f1");
    }

    @Test
    void testWithMultiplePrimaryKeyColumns() {
        util.tableEnv().createTable("T", sourceDescriptorWithTwoPrimaryKeys);
        util.verifyRelPlan("SELECT f0, f1 FROM T WHERE (f1 < 1 OR f2 > 10) AND f0 IS NOT NULL");
    }

    @Test
    void testOnlyProjection() {
        util.tableEnv().createTable("T", sourceDescriptor);
        util.verifyRelPlan("SELECT f1, f2 FROM T");
    }

    @Test
    void testFilterAndProjection() {
        util.tableEnv().createTable("T", sourceDescriptorWithTwoPrimaryKeys);
        util.verifyRelPlan("SELECT f1, f5 FROM T WHERE (f1 < 1 OR f2 > 10) AND f3 IS NOT NULL");
    }

    @Test
    void testPartialPrimaryKeyFilterAndProjection() {
        util.tableEnv().createTable("T", sourceDescriptorWithTwoPrimaryKeys);
        util.verifyRelPlan("SELECT f1, f5 FROM T WHERE f1 < 1 AND f3 IS NOT NULL");
    }

    @Test
    void testPartialPushDownWithTrimmedFieldsAndDifferentProjection() {
        util.tableEnv().createTable("T", sourceDescriptorWithTwoPrimaryKeys);
        // verifyExecPlan is intended here as it will show whether the node is reused or not
        util.verifyExecPlan(
                "SELECT f3 FROM T WHERE f2 < 1 AND f2 > 0\n"
                        + " UNION SELECT f3 FROM T WHERE f2 < 3 AND f2 > 0\n"
                        + " INTERSECT SELECT f3 FROM T WHERE f2 > 0 AND f2 < 10");
    }

    @Test
    void testPartialPushDownWithTrimmedFields() {
        util.tableEnv().createTable("T", sourceDescriptorWithTwoPrimaryKeys);
        // verifyExecPlan is intended here as it will show whether the node is reused or not
        util.verifyExecPlan(
                "SELECT f2 FROM T WHERE f2 < 1 AND f2 > 0\n"
                        + " UNION SELECT f2 FROM T WHERE f2 < 3 AND f2 > 0\n"
                        + " INTERSECT SELECT f2 FROM T WHERE f2 > 0 AND f2 < 10");
    }
}
