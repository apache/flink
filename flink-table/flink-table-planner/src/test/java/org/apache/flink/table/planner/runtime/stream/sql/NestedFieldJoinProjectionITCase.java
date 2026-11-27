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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the results of joins with nested field accesses that are planned with {@code
 * ProjectNestedFieldJoinTransposeRule}. The plans and the typing are covered by plan and unit
 * tests; this test is about the results at runtime, in particular with NULL structs and outer
 * joins.
 */
class NestedFieldJoinProjectionITCase extends StreamingTestBase {

    @BeforeEach
    void setupTables() {
        createTable(
                "L",
                "id int, nested row<name string, `value` int>",
                Row.of(1, Row.of("alice", 10)),
                Row.of(2, Row.of("bob", 20)));
        createTable(
                "R",
                "id int, info row<city string, zip int>",
                Row.of(1, Row.of("paris", 75000)),
                Row.of(2, Row.of("rome", 100)),
                Row.of(3, Row.of("tokyo", 10)));
    }

    @Test
    void testInnerJoinResults() throws Exception {
        final String query =
                "SELECT L.nested.name AS name, R.info.city AS city FROM L JOIN R ON L.id = R.id";

        assertStructsNotInJoin(query, "nested", "info");
        assertThat(collect(query)).containsExactlyInAnyOrder("+I[alice, paris]", "+I[bob, rome]");
    }

    @Test
    void testNotNullLeafOfNullableStructInCondition() throws Exception {
        // A NULL struct makes its NOT NULL leaf NULL, so `IS NULL` has to match for row 2.
        createTable(
                "LNN",
                "id int, nested row<v int not null, w string>",
                Row.of(1, Row.of(5, "a")),
                Row.of(2, null),
                Row.of(3, Row.of(7, "c")));
        final String query =
                "SELECT LNN.id, R.info.city AS city FROM LNN LEFT JOIN R "
                        + "ON LNN.id = R.id AND LNN.nested.v IS NULL";

        assertStructsNotInJoin(query, "nested", "info");
        assertThat(collect(query))
                .containsExactlyInAnyOrder("+I[1, null]", "+I[2, rome]", "+I[3, null]");
    }

    @Test
    void testLeftOuterJoinNotNullInnerFieldResults() throws Exception {
        // NOT NULL leaves of the null-producing side are widened to nullable by the join.
        createTable(
                "RNN",
                "id int not null, info row<city string not null, zip int not null> not null",
                Row.of(1, Row.of("paris", 10)),
                Row.of(2, Row.of("rome", 30)),
                Row.of(3, Row.of("tokyo", 40)));
        final String query =
                "SELECT L.nested.name AS name, RNN.info.city AS city FROM L "
                        + "LEFT JOIN RNN ON L.nested.`value` = RNN.info.zip";

        assertStructsNotInJoin(query, "nested", "info");
        assertThat(collect(query)).containsExactlyInAnyOrder("+I[alice, paris]", "+I[bob, null]");
    }

    @Test
    void testDeeplyNestedLeavesInExpressionsAndConditionWithNullStruct() throws Exception {
        createTable(
                "D",
                "id int, deep row<l1 row<l2 row<l3 int, x int>, y int>, z int>",
                Row.of(1, Row.of(Row.of(Row.of(100, 101), 102), 103)),
                Row.of(2, Row.of(Row.of(Row.of(200, 201), 202), 203)),
                Row.of(3, null));
        final String query =
                "SELECT D.id, D.deep.l1.l2.l3 + R.info.zip AS total, UPPER(R.info.city) AS city "
                        + "FROM D LEFT JOIN R ON D.id = R.id AND D.deep.l1.y > 150";

        assertStructsNotInJoin(query, "deep", "info");
        assertThat(collect(query))
                .containsExactlyInAnyOrder(
                        "+I[1, null, null]", "+I[2, 300, ROME]", "+I[3, null, null]");
    }

    @Test
    void testThreeWayJoinWithRightJoin() throws Exception {
        createTable(
                "S",
                "id int, extra row<tag string, n int>",
                Row.of(2, Row.of("s2", 20)),
                Row.of(3, Row.of("s3", 30)),
                Row.of(4, Row.of("s4", 40)));
        final String query =
                "SELECT L.nested.name AS name, R.info.city AS city, S.extra.tag AS tag FROM L "
                        + "JOIN R ON L.id = R.id RIGHT JOIN S ON S.id = R.id";

        assertStructsNotInJoin(query, "nested", "info", "extra");
        assertThat(collect(query))
                .containsExactlyInAnyOrder(
                        "+I[bob, rome, s2]", "+I[null, null, s3]", "+I[null, null, s4]");
    }

    @Test
    void testUpsertInputWithUpdatesAndDeletes() throws Exception {
        // The right side is the null-producing side of the join and receives updates and deletes.
        // The update of 1 changes the leaf that the query reads. The update of 2 changes only a
        // leaf that the query does not read, so the pushed projection of its rows is unchanged.
        createTable(
                "RU",
                "id int, info row<city string, zip int>, PRIMARY KEY (id) NOT ENFORCED",
                ", 'changelog-mode' = 'I,UA,D'",
                Row.of(1, Row.of("paris", 75000)),
                Row.of(2, Row.of("rome", 100)),
                Row.ofKind(RowKind.UPDATE_AFTER, 1, Row.of("lyon", 69000)),
                Row.ofKind(RowKind.UPDATE_AFTER, 2, Row.of("rome", 999)),
                Row.ofKind(RowKind.DELETE, 2, Row.of("rome", 999)));
        final String query =
                "SELECT L.nested.name AS name, RU.info.city AS city FROM L "
                        + "LEFT JOIN RU ON L.id = RU.id";

        assertStructsNotInJoin(query, "nested", "info");
        assertThat(collect(query)).containsExactlyInAnyOrder("+I[alice, lyon]", "+I[bob, null]");
    }

    @Test
    void testRetractInputWithUpdatesAndDeletes() throws Exception {
        // like above, for an input without a primary key that sends -U and -D rows
        createTable(
                "RR",
                "id int, info row<city string, zip int>",
                ", 'changelog-mode' = 'I,UB,UA,D'",
                Row.of(1, Row.of("paris", 75000)),
                Row.of(2, Row.of("rome", 100)),
                Row.ofKind(RowKind.UPDATE_BEFORE, 1, Row.of("paris", 75000)),
                Row.ofKind(RowKind.UPDATE_AFTER, 1, Row.of("lyon", 69000)),
                Row.ofKind(RowKind.UPDATE_BEFORE, 2, Row.of("rome", 100)),
                Row.ofKind(RowKind.UPDATE_AFTER, 2, Row.of("rome", 999)),
                Row.ofKind(RowKind.DELETE, 2, Row.of("rome", 999)));
        final String query =
                "SELECT L.nested.name AS name, RR.info.city AS city FROM L "
                        + "LEFT JOIN RR ON L.id = RR.id";

        assertStructsNotInJoin(query, "nested", "info");
        assertThat(collect(query)).containsExactlyInAnyOrder("+I[alice, lyon]", "+I[bob, null]");
    }

    private void createTable(String name, String columns, Row... rows) {
        createTable(name, columns, "", rows);
    }

    private void createTable(String name, String columns, String options, Row... rows) {
        final String dataId = TestValuesTableFactory.registerData(List.of(rows));
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE %s (%s) WITH ("
                                        + "'connector' = 'values', 'data-id' = '%s', 'bounded' = 'false'%s)",
                                name, columns, dataId, options));
    }

    /** Runs the query into a sink that accepts updates and returns the materialized result. */
    private List<String> collect(String query) throws Exception {
        tEnv().executeSql(
                        "CREATE TABLE sink WITH ('connector' = 'values', 'sink-insert-only' = 'false') AS "
                                + query)
                .await();
        return TestValuesTableFactory.getResultsAsStrings("sink");
    }

    /** Asserts that none of the given struct columns is carried by a join of the optimized plan. */
    private void assertStructsNotInJoin(String query, String... structs) {
        final String joins =
                tEnv().explainSql(query)
                        .lines()
                        .filter(line -> line.contains("Join("))
                        .collect(Collectors.joining("\n"));
        assertThat(joins)
                .isNotEmpty()
                .doesNotContainPattern("\\b(" + String.join("|", structs) + ")\\b");
    }
}
