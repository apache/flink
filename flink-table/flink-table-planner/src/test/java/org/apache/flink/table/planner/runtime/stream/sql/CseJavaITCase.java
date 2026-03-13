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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.typeutils.Types;
import org.apache.flink.table.planner.runtime.utils.CseTestFunctions;
import org.apache.flink.table.planner.runtime.utils.CseTestFunctions.TestCse2Func;
import org.apache.flink.table.planner.runtime.utils.CseTestFunctions.TestCse3Func;
import org.apache.flink.table.planner.runtime.utils.CseTestFunctions.TestCse4Func;
import org.apache.flink.table.planner.runtime.utils.CseTestFunctions.TestCseFunc;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for Common Sub-expression Elimination (CSE) optimization in Flink SQL.
 *
 * <p>CSE identifies duplicate sub-expressions in SQL projections and ensures each is evaluated only
 * once. The result is cached in a local variable and reused by subsequent references. This is
 * especially beneficial for expensive UDF calls.
 *
 * <p>Example: Given {@code SELECT f2(f(x)), f3(f(x)), f4(f(x)) FROM T}, the sub-expression {@code
 * f(x)} is evaluated once and reused 3 times, instead of being computed 3 times.
 */
class CseJavaITCase {

    private static final int PARALLELISM = 4;

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        CseTestFunctions.resetCounters();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        tEnv = StreamTableEnvironment.create(env, settings);
    }

    /**
     * Tests that common sub-expressions in projections produce correct results.
     *
     * <p>SQL: SELECT testcse2(testcse(c)), testcse3(testcse(c)), testcse4(testcse(c)) FROM input
     *
     * <p>Expected: testcse(c) is evaluated once per row; outer functions each see the cached
     * result.
     */
    @Test
    void testCseCorrectness() throws Exception {
        tEnv.createTemporarySystemFunction("testcse", TestCseFunc.class);
        tEnv.createTemporarySystemFunction("testcse2", TestCse2Func.class);
        tEnv.createTemporarySystemFunction("testcse3", TestCse3Func.class);
        tEnv.createTemporarySystemFunction("testcse4", TestCse4Func.class);

        TypeInformation<Row> typeInfo = new RowTypeInfo(Types.STRING());
        DataStream<Row> ds =
                env.fromCollection(
                        Arrays.asList(Row.of("hello"), Row.of("world"), Row.of("flink")), typeInfo);

        Table t = tEnv.fromDataStream(ds).as("c");
        tEnv.createTemporaryView("input", t);

        String sqlQuery =
                "SELECT "
                        + "  testcse2(testcse(c)) AS region1, "
                        + "  testcse3(testcse(c)) AS region2, "
                        + "  testcse4(testcse(c)) AS region3 "
                        + "FROM input";

        Table resultTable = tEnv.sqlQuery(sqlQuery);
        List<String> results = collectResults(resultTable);
        Collections.sort(results);

        List<String> expected =
                Arrays.asList(
                        "FLINK_2,FLINK_3,FLINK_4",
                        "HELLO_2,HELLO_3,HELLO_4",
                        "WORLD_2,WORLD_3,WORLD_4");
        Collections.sort(expected);
        assertEquals(expected, results);
    }

    /**
     * Tests that the inner common sub-expression (testcse) is called only once per row when CSE
     * optimization is active. Without CSE it would be called 3 times per row.
     */
    @Test
    void testCseCallCount() throws Exception {
        tEnv.createTemporarySystemFunction("testcse", TestCseFunc.class);
        tEnv.createTemporarySystemFunction("testcse2", TestCse2Func.class);
        tEnv.createTemporarySystemFunction("testcse3", TestCse3Func.class);
        tEnv.createTemporarySystemFunction("testcse4", TestCse4Func.class);

        TypeInformation<Row> typeInfo = new RowTypeInfo(Types.STRING());
        DataStream<Row> ds = env.fromCollection(Arrays.asList(Row.of("a"), Row.of("b")), typeInfo);

        Table t = tEnv.fromDataStream(ds).as("c");
        tEnv.createTemporaryView("input", t);

        String sqlQuery =
                "SELECT "
                        + "  testcse2(testcse(c)) AS r1, "
                        + "  testcse3(testcse(c)) AS r2, "
                        + "  testcse4(testcse(c)) AS r3 "
                        + "FROM input";

        Table resultTable = tEnv.sqlQuery(sqlQuery);
        List<String> results = collectResults(resultTable);

        // With CSE: testcse should be called once per row (total 2 for 2 rows)
        // Without CSE: testcse would be called 3 times per row (total 6 for 2 rows)
        int totalCseCalls = TestCseFunc.CALL_COUNT.get();
        assertEquals(
                2,
                totalCseCalls,
                "testcse should be called once per row due to CSE, "
                        + "not 3 times. Total calls for 2 rows should be 2, but got "
                        + totalCseCalls);

        // Each wrapper function should also be called once per row (total 2 each)
        assertEquals(2, TestCse2Func.CALL_COUNT.get());
        assertEquals(2, TestCse3Func.CALL_COUNT.get());
        assertEquals(2, TestCse4Func.CALL_COUNT.get());
    }

    /**
     * Tests CSE with no duplicate sub-expressions. All expressions are unique, so CSE should not
     * affect the result.
     */
    @Test
    void testNoCseCandidates() throws Exception {
        tEnv.createTemporarySystemFunction("testcse", TestCseFunc.class);
        tEnv.createTemporarySystemFunction("testcse2", TestCse2Func.class);
        tEnv.createTemporarySystemFunction("testcse3", TestCse3Func.class);

        TypeInformation<Row> typeInfo = new RowTypeInfo(Types.STRING(), Types.STRING());
        DataStream<Row> ds =
                env.fromCollection(Collections.singletonList(Row.of("hello", "world")), typeInfo);

        Table t = tEnv.fromDataStream(ds).as("a", "b");
        tEnv.createTemporaryView("input", t);

        // No duplicates: testcse(a) and testcse(b) are different expressions
        String sqlQuery =
                "SELECT "
                        + "  testcse2(testcse(a)) AS r1, "
                        + "  testcse3(testcse(b)) AS r2 "
                        + "FROM input";

        Table resultTable = tEnv.sqlQuery(sqlQuery);
        List<String> results = collectResults(resultTable);

        List<String> expected = Collections.singletonList("HELLO_2,WORLD_3");
        assertEquals(expected, results);
    }

    /**
     * Tests CSE with null handling. The common sub-expression result may be null, and the CSE
     * mechanism must correctly propagate null flags.
     */
    @Test
    void testCseWithNullValues() throws Exception {
        tEnv.createTemporarySystemFunction("testcse", TestCseFunc.class);
        tEnv.createTemporarySystemFunction("testcse2", TestCse2Func.class);
        tEnv.createTemporarySystemFunction("testcse3", TestCse3Func.class);

        TypeInformation<Row> typeInfo = new RowTypeInfo(Types.STRING());
        DataStream<Row> ds =
                env.fromCollection(Arrays.asList(Row.of("hello"), Row.of((String) null)), typeInfo);

        Table t = tEnv.fromDataStream(ds).as("c");
        tEnv.createTemporaryView("input", t);

        String sqlQuery =
                "SELECT "
                        + "  testcse2(testcse(c)) AS r1, "
                        + "  testcse3(testcse(c)) AS r2 "
                        + "FROM input";

        Table resultTable = tEnv.sqlQuery(sqlQuery);
        List<String> results = collectResults(resultTable);
        Collections.sort(results);

        List<String> expected = Arrays.asList("HELLO_2,HELLO_3", "null,null");
        Collections.sort(expected);
        assertEquals(expected, results);
    }

    /**
     * Tests CSE with deeply nested common sub-expressions.
     *
     * <p>SQL: SELECT testcse2(testcse(testcse(c))), testcse3(testcse(testcse(c))) FROM input
     *
     * <p>Here both testcse(c) and testcse(testcse(c)) are common sub-expressions.
     */
    @Test
    void testCseWithNestedCommonSubExpressions() throws Exception {
        tEnv.createTemporarySystemFunction("testcse", TestCseFunc.class);
        tEnv.createTemporarySystemFunction("testcse2", TestCse2Func.class);
        tEnv.createTemporarySystemFunction("testcse3", TestCse3Func.class);

        TypeInformation<Row> typeInfo = new RowTypeInfo(Types.STRING());
        DataStream<Row> ds = env.fromCollection(Collections.singletonList(Row.of("hi")), typeInfo);

        Table t = tEnv.fromDataStream(ds).as("c");
        tEnv.createTemporaryView("input", t);

        String sqlQuery =
                "SELECT "
                        + "  testcse2(testcse(testcse(c))) AS r1, "
                        + "  testcse3(testcse(testcse(c))) AS r2 "
                        + "FROM input";

        Table resultTable = tEnv.sqlQuery(sqlQuery);
        List<String> results = collectResults(resultTable);

        // testcse("hi") = "HI", testcse("HI") = "HI" (already uppercase)
        // testcse2("HI") = "HI_2", testcse3("HI") = "HI_3"
        List<String> expected = Collections.singletonList("HI_2,HI_3");
        assertEquals(expected, results);
    }

    /** Collects all results from a table into a list of comma-separated strings. */
    private List<String> collectResults(Table resultTable) throws Exception {
        List<String> results = new ArrayList<>();
        try (CloseableIterator<Row> it = resultTable.execute().collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < row.getArity(); i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    sb.append(row.getField(i));
                }
                results.add(sb.toString());
            }
        }
        return results;
    }
}
