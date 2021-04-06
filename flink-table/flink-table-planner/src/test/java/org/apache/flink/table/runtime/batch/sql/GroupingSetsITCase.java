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

package org.apache.flink.table.runtime.batch.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.runtime.utils.TableProgramsClusterTestBase;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Comparator;
import java.util.List;

/**
 * This test should be replaced by a DataSetAggregateITCase. We should only perform logical unit
 * tests here. Until then, we perform a cluster test here.
 */
@RunWith(Parameterized.class)
public class GroupingSetsITCase extends TableProgramsClusterTestBase {

    private static final String TABLE_NAME = "MyTable";
    private static final String TABLE_WITH_NULLS_NAME = "MyTableWithNulls";
    private BatchTableEnvironment tableEnv;

    public GroupingSetsITCase(TestExecutionMode mode, TableConfigMode tableConfigMode) {
        super(mode, tableConfigMode);
    }

    @Before
    public void setupTables() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        tableEnv = BatchTableEnvironment.create(env, new TableConfig());

        DataSet<Tuple3<Integer, Long, String>> dataSet = CollectionDataSets.get3TupleDataSet(env);
        tableEnv.createTemporaryView(TABLE_NAME, dataSet);

        MapOperator<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> dataSetWithNulls =
                dataSet.map(
                        new MapFunction<
                                Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>>() {

                            @Override
                            public Tuple3<Integer, Long, String> map(
                                    Tuple3<Integer, Long, String> value) throws Exception {
                                if (value.f2.toLowerCase().contains("world")) {
                                    value.f2 = null;
                                }
                                return value;
                            }
                        });
        tableEnv.createTemporaryView(TABLE_WITH_NULLS_NAME, dataSetWithNulls);
    }

    @Test
    public void testGroupingSets() throws Exception {
        String query =
                "SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, "
                        + " GROUPING(f1) as gf1, GROUPING(f2) as gf2, "
                        + " GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, "
                        + " GROUPING_ID(f1, f2) as gid, "
                        + " COUNT(*) as cnt"
                        + " FROM "
                        + TABLE_NAME
                        + " GROUP BY GROUPING SETS (f1, f2, ())";

        String expected =
                "1,null,1,0,1,0,1,0,2,1\n"
                        + "2,null,2,0,1,0,1,0,2,2\n"
                        + "3,null,5,0,1,0,1,0,2,3\n"
                        + "4,null,8,0,1,0,1,0,2,4\n"
                        + "5,null,13,0,1,0,1,0,2,5\n"
                        + "6,null,18,0,1,0,1,0,2,6\n"
                        + "null,Comment#1,7,0,0,1,0,1,1,1\n"
                        + "null,Comment#10,16,0,0,1,0,1,1,1\n"
                        + "null,Comment#11,17,0,0,1,0,1,1,1\n"
                        + "null,Comment#12,18,0,0,1,0,1,1,1\n"
                        + "null,Comment#13,19,0,0,1,0,1,1,1\n"
                        + "null,Comment#14,20,0,0,1,0,1,1,1\n"
                        + "null,Comment#15,21,0,0,1,0,1,1,1\n"
                        + "null,Comment#2,8,0,0,1,0,1,1,1\n"
                        + "null,Comment#3,9,0,0,1,0,1,1,1\n"
                        + "null,Comment#4,10,0,0,1,0,1,1,1\n"
                        + "null,Comment#5,11,0,0,1,0,1,1,1\n"
                        + "null,Comment#6,12,0,0,1,0,1,1,1\n"
                        + "null,Comment#7,13,0,0,1,0,1,1,1\n"
                        + "null,Comment#8,14,0,0,1,0,1,1,1\n"
                        + "null,Comment#9,15,0,0,1,0,1,1,1\n"
                        + "null,Hello world, how are you?,4,0,0,1,0,1,1,1\n"
                        + "null,Hello world,3,0,0,1,0,1,1,1\n"
                        + "null,Hello,2,0,0,1,0,1,1,1\n"
                        + "null,Hi,1,0,0,1,0,1,1,1\n"
                        + "null,I am fine.,5,0,0,1,0,1,1,1\n"
                        + "null,Luke Skywalker,6,0,0,1,0,1,1,1\n"
                        + "null,null,11,0,0,0,0,0,0,21";

        checkSql(query, expected);
    }

    @Test
    public void testGroupingSetsWithNulls() throws Exception {
        String query =
                "SELECT f1, f2, avg(f0) as a, GROUP_ID() as g FROM "
                        + TABLE_WITH_NULLS_NAME
                        + " GROUP BY GROUPING SETS (f1, f2)";

        String expected =
                "1,Hi,1,0\n"
                        + "2,Hello,2,0\n"
                        + "2,null,3,0\n"
                        + "3,I am fine.,5,0\n"
                        + "3,Luke Skywalker,6,0\n"
                        + "3,null,4,0\n"
                        + "4,Comment#1,7,0\n"
                        + "4,Comment#2,8,0\n"
                        + "4,Comment#3,9,0\n"
                        + "4,Comment#4,10,0\n"
                        + "5,Comment#5,11,0\n"
                        + "5,Comment#6,12,0\n"
                        + "5,Comment#7,13,0\n"
                        + "5,Comment#8,14,0\n"
                        + "5,Comment#9,15,0\n"
                        + "6,Comment#10,16,0\n"
                        + "6,Comment#11,17,0\n"
                        + "6,Comment#12,18,0\n"
                        + "6,Comment#13,19,0\n"
                        + "6,Comment#14,20,0\n"
                        + "6,Comment#15,21,0";

        checkSql(query, expected);
    }

    @Test
    public void testCubeAsGroupingSets() throws Exception {
        String cubeQuery =
                "SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, "
                        + " GROUPING(f1) as gf1, GROUPING(f2) as gf2, "
                        + " GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, "
                        + " GROUPING_ID(f1, f2) as gid "
                        + " FROM "
                        + TABLE_NAME
                        + " GROUP BY CUBE (f1, f2)";

        String groupingSetsQuery =
                "SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, "
                        + " GROUPING(f1) as gf1, GROUPING(f2) as gf2, "
                        + " GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, "
                        + " GROUPING_ID(f1, f2) as gid "
                        + " FROM "
                        + TABLE_NAME
                        + " GROUP BY GROUPING SETS ((f1, f2), (f1), (f2), ())";

        compareSql(cubeQuery, groupingSetsQuery);
    }

    @Test
    public void testRollupAsGroupingSets() throws Exception {
        String rollupQuery =
                "SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, "
                        + " GROUPING(f1) as gf1, GROUPING(f2) as gf2, "
                        + " GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, "
                        + " GROUPING_ID(f1, f2) as gid "
                        + " FROM "
                        + TABLE_NAME
                        + " GROUP BY ROLLUP (f1, f2)";

        String groupingSetsQuery =
                "SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, "
                        + " GROUPING(f1) as gf1, GROUPING(f2) as gf2, "
                        + " GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, "
                        + " GROUPING_ID(f1, f2) as gid "
                        + " FROM "
                        + TABLE_NAME
                        + " GROUP BY GROUPING SETS ((f1, f2), (f1), ())";

        compareSql(rollupQuery, groupingSetsQuery);
    }

    /**
     * Execute SQL query and check results.
     *
     * @param query SQL query.
     * @param expected Expected result.
     */
    private void checkSql(String query, String expected) throws Exception {
        Table resultTable = tableEnv.sqlQuery(query);
        DataSet<Row> resultDataSet = tableEnv.toDataSet(resultTable, Row.class);
        List<Row> results = resultDataSet.collect();
        TestBaseUtils.compareResultAsText(results, expected);
    }

    private void compareSql(String query1, String query2) throws Exception {

        // Function to map row to string
        MapFunction<Row, String> mapFunction =
                new MapFunction<Row, String>() {

                    @Override
                    public String map(Row value) throws Exception {
                        return value == null ? "null" : value.toString();
                    }
                };

        // Execute first query and store results
        Table resultTable1 = tableEnv.sqlQuery(query1);
        DataSet<Row> resultDataSet1 = tableEnv.toDataSet(resultTable1, Row.class);
        List<String> results1 = resultDataSet1.map(mapFunction).collect();

        // Execute second query and store results
        Table resultTable2 = tableEnv.sqlQuery(query2);
        DataSet<Row> resultDataSet2 = tableEnv.toDataSet(resultTable2, Row.class);
        List<String> results2 = resultDataSet2.map(mapFunction).collect();

        // Compare results
        TestBaseUtils.compareResultCollections(
                results1,
                results2,
                new Comparator<String>() {

                    @Override
                    public int compare(String o1, String o2) {
                        return o2 == null ? o1 == null ? 0 : 1 : o1.compareTo(o2);
                    }
                });
    }
}
