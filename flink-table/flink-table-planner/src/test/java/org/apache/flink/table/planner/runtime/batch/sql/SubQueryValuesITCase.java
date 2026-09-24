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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.Row;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

/** Execution tests for correlated subqueries over SQL VALUES. */
class SubQueryValuesITCase extends BatchTestBase {

    private static final String LEFT_VALUES =
            "(VALUES (1), (1), (2), (2), (3), (CAST(NULL AS INT))) AS l(id)";

    @ParameterizedTest(name = "anti={0}")
    @ValueSource(booleans = {false, true})
    void testNullsAndDuplicates(boolean anti) {
        final String query =
                query(anti, LEFT_VALUES, "(VALUES (1), (1), (CAST(NULL AS INT))) AS r(id)");
        if (anti) {
            checkRows(query, Row.of(2), Row.of(2), Row.of(3), Row.of((Object) null));
        } else {
            checkRows(query, Row.of(1), Row.of(1));
        }
    }

    @ParameterizedTest(name = "anti={0}")
    @ValueSource(booleans = {false, true})
    void testOnlyNullsOnRight(boolean anti) {
        final String query =
                query(
                        anti,
                        LEFT_VALUES,
                        "(VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT))) AS r(id)");
        if (anti) {
            checkRows(
                    query,
                    Row.of(1),
                    Row.of(1),
                    Row.of(2),
                    Row.of(2),
                    Row.of(3),
                    Row.of((Object) null));
        } else {
            checkRows(query);
        }
    }

    @ParameterizedTest(name = "anti={0}")
    @ValueSource(booleans = {false, true})
    void testEmptyRight(boolean anti) {
        final String query =
                query(
                        anti,
                        LEFT_VALUES,
                        "(SELECT id FROM (VALUES (1), (2)) AS v(id) WHERE FALSE) AS r");
        if (anti) {
            checkRows(
                    query,
                    Row.of(1),
                    Row.of(1),
                    Row.of(2),
                    Row.of(2),
                    Row.of(3),
                    Row.of((Object) null));
        } else {
            checkRows(query);
        }
    }

    @ParameterizedTest(name = "anti={0}")
    @ValueSource(booleans = {false, true})
    void testEmptyLeft(boolean anti) {
        checkRows(
                query(
                        anti,
                        "(SELECT id FROM (VALUES (1), (2)) AS v(id) WHERE FALSE) AS l",
                        "(VALUES (1), (2)) AS r(id)"));
    }

    @ParameterizedTest(name = "anti={0}")
    @ValueSource(booleans = {false, true})
    void testProjectAndFilter(boolean anti) {
        final String query =
                "SELECT l.id, l.amount "
                        + "FROM (VALUES (1, 10), (1, 30), (2, 20), (3, 40), (CAST(NULL AS INT), 50)) "
                        + "AS l(id, amount) WHERE "
                        + (anti ? "NOT EXISTS" : "EXISTS")
                        + " (SELECT 1 FROM ("
                        + "SELECT raw_id + 1 AS id, limit_value "
                        + "FROM (VALUES (25, 0), (10, 1), (CAST(NULL AS INT), 2)) "
                        + "AS v(limit_value, raw_id) "
                        + "WHERE limit_value IS NOT NULL) AS r "
                        + "WHERE l.id = r.id AND l.amount < r.limit_value)";
        if (anti) {
            checkRows(query, Row.of(1, 30), Row.of(2, 20), Row.of(3, 40), Row.of(null, 50));
        } else {
            checkRows(query, Row.of(1, 10));
        }
    }

    private static String query(boolean anti, String left, String right) {
        return "SELECT l.id FROM "
                + left
                + " WHERE "
                + (anti ? "NOT EXISTS" : "EXISTS")
                + " (SELECT 1 FROM "
                + right
                + " WHERE l.id = r.id)";
    }

    private void checkRows(String query, Row... expected) {
        checkResult(query, JavaScalaConversionUtil.toScala(Arrays.asList(expected)), false);
    }
}
