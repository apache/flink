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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Plan tests for correlated subqueries over SQL VALUES. */
class SubQueryValuesTest extends TableTestBase {

    private BatchTableTestUtil util;

    @BeforeEach
    void setup() {
        util = batchTestUtil(TableConfig.getDefault());
        util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"EXISTS", "NOT EXISTS"})
    void testValues(String predicate) {
        util.verifyRelPlan(query(predicate));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"EXISTS", "NOT EXISTS"})
    void testProjectAndFilter(String predicate) {
        util.verifyRelPlan(queryWithProjectAndFilter(predicate));
    }

    private static String query(String predicate) {
        return "SELECT l.id FROM (VALUES (1), (2)) AS l(id) WHERE "
                + predicate
                + " (SELECT 1 FROM (VALUES (1), (2)) AS r(right_id) WHERE l.id = r.right_id)";
    }

    private static String queryWithProjectAndFilter(String predicate) {
        return "SELECT l.id, l.amount "
                + "FROM (VALUES (1, 10), (1, 30), (2, 20), (3, 40), (CAST(NULL AS INT), 50)) "
                + "AS l(id, amount) "
                + "WHERE "
                + predicate
                + " (SELECT 1 FROM ("
                + "SELECT raw_id + 1 AS id, limit_value "
                + "FROM (VALUES (25, 0), (10, 1), (CAST(NULL AS INT), 2)) AS v(limit_value, raw_id) "
                + "WHERE limit_value IS NOT NULL) AS r "
                + "WHERE l.id = r.id AND l.amount < r.limit_value)";
    }
}
