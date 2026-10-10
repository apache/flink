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
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Tests for OVER windows ordered by a non-time attribute. */
@ExtendWith(ParameterizedTestExtension.class)
public class NonTimeOverAggregateITCase extends StreamingWithStateTestBase {

    public NonTimeOverAggregateITCase(StateBackendMode state) {
        super(state);
    }

    @Parameters(name = "backend = {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[][] {
                    {StreamingWithStateTestBase.HEAP_BACKEND()},
                    {StreamingWithStateTestBase.ROCKSDB_BACKEND()}
                });
    }

    @BeforeEach
    public void before() {
        super.before();
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of("a", 30, "p"), Row.of("a", 20, "q"), Row.of("a", 10, "r")));
        tEnv().executeSql(
                        "CREATE TABLE src (k STRING, ord INT, v STRING) WITH ("
                                + " 'connector' = 'values',"
                                + " 'data-id' = '"
                                + dataId
                                + "',"
                                + " 'bounded' = 'true')");
        String repeatedDataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of("a", 30, "p"),
                                Row.of("a", 10, "r"),
                                Row.of("a", 20, "q"),
                                Row.of("a", 25, "r")));
        tEnv().executeSql(
                        "CREATE TABLE src_repeated (k STRING, ord INT, v STRING) WITH ("
                                + " 'connector' = 'values',"
                                + " 'data-id' = '"
                                + repeatedDataId
                                + "',"
                                + " 'bounded' = 'true')");
    }

    @TestTemplate
    void testLag() throws Exception {
        assertThat(currentValues("LAG(v, 1)"))
                .containsExactly(entry(10, "null"), entry(20, "r"), entry(30, "q"));
    }

    @TestTemplate
    void testLagInsertsInBetween() throws Exception {
        assertThat(currentValues("src_repeated", "LAG(v, 1)"))
                .containsExactly(entry(10, "null"), entry(20, "r"), entry(25, "q"), entry(30, "r"));
    }

    @TestTemplate
    void testArrayAgg() throws Exception {
        assertThat(currentValues("ARRAY_AGG(v)"))
                .containsExactly(entry(10, "[r]"), entry(20, "[r, q]"), entry(30, "[r, q, p]"));
    }

    @TestTemplate
    void testArrayAggInsertsInBetween() throws Exception {
        assertThat(currentValues("src_repeated", "ARRAY_AGG(v)"))
                .containsExactly(
                        entry(10, "[r]"),
                        entry(20, "[r, q]"),
                        entry(25, "[r, q, r]"),
                        entry(30, "[r, q, r, p]"));
    }

    @TestTemplate
    void testBitmapBuildCardinalityInsertsInBetween() throws Exception {
        assertThat(currentValues("src_repeated", "BITMAP_BUILD_CARDINALITY_AGG(ord)"))
                .containsExactly(entry(10, "1"), entry(20, "2"), entry(25, "3"), entry(30, "4"));
    }

    @TestTemplate
    void testRetractionsOfInsertInBetween() throws Exception {
        assertThat(changelog("src_repeated", "ARRAY_AGG(v)"))
                .containsExactly(
                        "+I 30 [p]",
                        "+I 10 [r]",
                        "-U 30 [p]",
                        "+U 30 [r, p]",
                        "+I 20 [r, q]",
                        "-U 30 [r, p]",
                        "+U 30 [r, q, p]",
                        "+I 25 [r, q, r]",
                        "-U 30 [r, q, p]",
                        "+U 30 [r, q, r, p]");
    }

    private static String overSql(String table, String agg) {
        return "SELECT ord, " + agg + " OVER (PARTITION BY k ORDER BY ord) FROM " + table;
    }

    /** Returns the raw changelog, so that retracted values are visible too. */
    private List<String> changelog(String table, String agg) throws Exception {
        List<String> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv().executeSql(overSql(table, agg)).collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                rows.add(
                        row.getKind().shortString()
                                + " "
                                + row.getField(0)
                                + " "
                                + asString(row.getField(1)));
            }
        }
        return rows;
    }

    private static String asString(Object value) {
        return value instanceof Object[]
                ? Arrays.toString((Object[]) value)
                : String.valueOf(value);
    }

    private Map<Object, String> currentValues(String agg) throws Exception {
        return currentValues("src", agg);
    }

    /** Applies the changelog, keyed on the ordering column, and returns the resulting table. */
    private Map<Object, String> currentValues(String table, String agg) throws Exception {
        Map<Object, String> current = new TreeMap<>();
        try (CloseableIterator<Row> it = tEnv().executeSql(overSql(table, agg)).collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                if (row.getKind() == RowKind.DELETE) {
                    current.remove(row.getField(0));
                } else if (row.getKind() != RowKind.UPDATE_BEFORE) {
                    current.put(row.getField(0), asString(row.getField(1)));
                }
            }
        }
        return current;
    }
}
