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

package org.apache.flink.connectors.hive;

import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.Test;

import java.util.List;

import static org.apache.flink.table.planner.utils.TableTestUtil.readFromResource;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for native hive agg function compatibility. */
public class HiveDialectAggITCase extends HiveDialectITCaseBase {

    @Test
    public void testSumAggFunctionPlan() {
        // test explain
        String actualPlan = explainSql("select x, sum(y) from foo group by x");
        assertThat(actualPlan).isEqualTo(readFromResource("/explain/testSumAggFunctionPlan.out"));
    }

    @Test
    public void testSimpleSumAggFunction() throws Exception {
        tableEnv.executeSql("create table test(x string, y string, z int, d decimal(10,5))");
        tableEnv.executeSql(
                        "insert into test values (NULL, '2', 1, 1.11), (NULL, NULL, 2, 2.22), (NULL, '4', 3, 3.33), (NULL, NULL, 4, 4.45)")
                .await();

        // test sum with all elements are null
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select sum(x) from test").collect());
        assertThat(result.toString()).isEqualTo("[+I[null]]");

        // test sum string type with partial element is null, result type is double
        List<Row> result2 =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select sum(y) from test").collect());
        assertThat(result2.toString()).isEqualTo("[+I[6.0]]");

        // TODO test sum string with some string can't convert to bigint after FLINK-30221

        // test decimal type
        List<Row> result3 =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select sum(d) from test").collect());
        assertThat(result3.toString()).isEqualTo("[+I[11.11000]]");

        // test sum int, result type is bigint
        List<Row> result4 =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select sum(z) from test").collect());
        assertThat(result4.toString()).isEqualTo("[+I[10]]");

        // test sum string&int type simultaneously
        List<Row> result5 =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select sum(y), sum(z) from test").collect());
        assertThat(result5.toString()).isEqualTo("[+I[6.0, 10]]");

        tableEnv.executeSql("drop table test");
    }

    @Test
    public void testSumAggWithGroupKey() throws Exception {
        tableEnv.executeSql("create table test(name string, num bigint, price decimal(10,5))");
        tableEnv.executeSql(
                        "insert into test values ('tom', 2, 7.2), ('tony', 2, 23.7), ('tom', 10, 3.33), ('tony', 4, 4.45), ('nadal', 4, 10.455)")
                .await();

        List<Row> result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "select name, sum(num), sum(price),  sum(num * price) from test group by name")
                                .collect());
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[tom, 12, 10.53000, 47.70000], +I[tony, 6, 28.15000, 65.20000], +I[nadal, 4, 10.45500, 41.82000]]");

        tableEnv.executeSql("drop table test");
    }
}
