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

package org.apache.flink.table.runtime.stream.sql;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.JavaStreamTestData;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.table.utils.LegacyRowResource;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/** Integration tests for streaming SQL. */
public class JavaSqlITCase extends AbstractTestBase {

    @ClassRule public static LegacyRowResource usesLegacyRows = LegacyRowResource.INSTANCE;

    @Test
    public void testRowRegisterRowWithNames() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        StreamITCase.clear();

        List<Row> data = new ArrayList<>();
        data.add(Row.of(1, 1L, "Hi"));
        data.add(Row.of(2, 2L, "Hello"));
        data.add(Row.of(3, 2L, "Hello world"));

        TypeInformation<?>[] types = {
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.LONG_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO
        };
        String[] names = {"a", "b", "c"};

        RowTypeInfo typeInfo = new RowTypeInfo(types, names);

        DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);

        Table in = tableEnv.fromDataStream(ds, $("a"), $("b"), $("c"));
        tableEnv.registerTable("MyTableRow", in);

        String sqlQuery = "SELECT a,c FROM MyTableRow";
        Table result = tableEnv.sqlQuery(sqlQuery);

        DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
        resultSet.addSink(new StreamITCase.StringSink<Row>());
        env.execute();

        List<String> expected = new ArrayList<>();
        expected.add("1,Hi");
        expected.add("2,Hello");
        expected.add("3,Hello world");

        StreamITCase.compareWithList(expected);
    }

    @Test
    public void testSelect() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        StreamITCase.clear();

        DataStream<Tuple3<Integer, Long, String>> ds =
                JavaStreamTestData.getSmall3TupleDataSet(env);
        Table in = tableEnv.fromDataStream(ds, $("a"), $("b"), $("c"));
        tableEnv.registerTable("MyTable", in);

        String sqlQuery = "SELECT * FROM MyTable";
        Table result = tableEnv.sqlQuery(sqlQuery);

        DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
        resultSet.addSink(new StreamITCase.StringSink<Row>());
        env.execute();

        List<String> expected = new ArrayList<>();
        expected.add("1,1,Hi");
        expected.add("2,2,Hello");
        expected.add("3,2,Hello world");

        StreamITCase.compareWithList(expected);
    }

    @Test
    public void testFilter() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        StreamITCase.clear();

        DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds =
                JavaStreamTestData.get5TupleDataStream(env);
        tableEnv.createTemporaryView("MyTable", ds, $("a"), $("b"), $("c"), $("d"), $("e"));

        String sqlQuery = "SELECT a, b, e FROM MyTable WHERE c < 4";
        Table result = tableEnv.sqlQuery(sqlQuery);

        DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
        resultSet.addSink(new StreamITCase.StringSink<Row>());
        env.execute();

        List<String> expected = new ArrayList<>();
        expected.add("1,1,1");
        expected.add("2,2,2");
        expected.add("2,3,1");
        expected.add("3,4,2");

        StreamITCase.compareWithList(expected);
    }

    @Test
    public void testUnion() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        StreamITCase.clear();

        DataStream<Tuple3<Integer, Long, String>> ds1 =
                JavaStreamTestData.getSmall3TupleDataSet(env);
        Table t1 = tableEnv.fromDataStream(ds1, $("a"), $("b"), $("c"));
        tableEnv.registerTable("T1", t1);

        DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                JavaStreamTestData.get5TupleDataStream(env);
        tableEnv.createTemporaryView("T2", ds2, $("a"), $("b"), $("d"), $("c"), $("e"));

        String sqlQuery =
                "SELECT * FROM T1 " + "UNION ALL " + "(SELECT a, b, c FROM T2 WHERE a	< 3)";
        Table result = tableEnv.sqlQuery(sqlQuery);

        DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
        resultSet.addSink(new StreamITCase.StringSink<Row>());
        env.execute();

        List<String> expected = new ArrayList<>();
        expected.add("1,1,Hi");
        expected.add("2,2,Hello");
        expected.add("3,2,Hello world");
        expected.add("1,1,Hallo");
        expected.add("2,2,Hallo Welt");
        expected.add("2,3,Hallo Welt wie");

        StreamITCase.compareWithList(expected);
    }
}
