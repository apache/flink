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

package org.apache.flink.table.planner.plan.hints.batch;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that the batch planner ignores the EARLY_FIRE join hint. */
class EarlyFireJoinHintTest extends TableTestBase {

    private static final String INTERVAL_JOIN_QUERY =
            "SELECT %s o.id, s.id FROM orders o %s shipments s\n"
                    + "ON o.id = s.id AND o.ts BETWEEN s.ts - INTERVAL '1' MINUTE AND s.ts";

    private static final String EARLY_FIRE_HINT = "/*+ EARLY_FIRE('delay'='5s') */";

    private BatchTableTestUtil util;

    @BeforeEach
    void before() {
        util = batchTestUtil(TableConfig.getDefault());
        // the test util turns adaptive joins off; turn them back on as in the default config
        util.tableEnv()
                .getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
                        OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.AUTO);
        for (String table : new String[] {"orders", "shipments"}) {
            util.tableEnv()
                    .executeSql(
                            "CREATE TABLE "
                                    + table
                                    + " (\n"
                                    + "  id BIGINT,\n"
                                    + "  ts TIMESTAMP(3),\n"
                                    + "  WATERMARK FOR ts AS ts\n"
                                    + ") WITH (\n"
                                    + "  'connector' = 'values',\n"
                                    + "  'bounded' = 'true'\n"
                                    + ")");
        }
    }

    @Test
    void testLeftJoinPlansAsWithoutHint() {
        assertPlansAsWithoutHint("LEFT JOIN");
    }

    @Test
    void testInnerJoinPlansAsWithoutHint() {
        assertPlansAsWithoutHint("JOIN");
    }

    @Test
    void testStrategyHintStillAppliesWithEarlyFire() {
        String withEarlyFire =
                optimizedPlan(
                        String.format(
                                INTERVAL_JOIN_QUERY,
                                "/*+ EARLY_FIRE('delay'='5s'), SHUFFLE_MERGE(o) */",
                                "JOIN"));
        String strategyOnly =
                optimizedPlan(
                        String.format(INTERVAL_JOIN_QUERY, "/*+ SHUFFLE_MERGE(o) */", "JOIN"));
        assertThat(withEarlyFire).isEqualTo(strategyOnly).doesNotContain("AdaptiveJoin(");
    }

    private void assertPlansAsWithoutHint(String join) {
        String withHint = optimizedPlan(String.format(INTERVAL_JOIN_QUERY, EARLY_FIRE_HINT, join));
        String withoutHint = optimizedPlan(String.format(INTERVAL_JOIN_QUERY, "", join));
        // without an adaptive join here, the comparison would not cover the adaptive conversion
        assertThat(withoutHint).contains("AdaptiveJoin(");
        assertThat(withHint).isEqualTo(withoutHint);
    }

    /** Drops the abstract syntax tree, the only EXPLAIN section that prints the hint itself. */
    private String optimizedPlan(String sql) {
        String explain = util.tableEnv().explainSql(sql);
        return explain.substring(explain.indexOf("== Optimized Physical Plan =="));
    }
}
