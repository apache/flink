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

package org.apache.flink.api.java.sql.test;

import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.scala.table.streaming.test.utils.StreamITCase;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.api.java.table.test.utils.StreamTestData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class StreamingSQLITCase extends StreamingMultipleProgramsTestBase {

	@Test
	public void testSelect() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		DataStream<Tuple3<Integer, Long, String>> ds = StreamTestData.getSmall3TupleDataSet(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT STREAM * FROM MyTable";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList();
		expected.add("1,1,Hi");
		expected.add("2,2,Hello");
		expected.add("3,2,Hello world");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testFilter() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = StreamTestData.get5TupleDataStream(env);
		tableEnv.registerDataStream("MyTable", ds, "a, b, c, d, e");

		String sqlQuery = "SELECT STREAM a, b, e FROM MyTable WHERE c < 4";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList();
		expected.add("1,1,1");
		expected.add("2,2,2");
		expected.add("2,3,1");
		expected.add("3,4,2");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testUnion() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		DataStream<Tuple3<Integer, Long, String>> ds1 = StreamTestData.getSmall3TupleDataSet(env);
		Table t1 = tableEnv.fromDataStream(ds1, "a,b,c");
		tableEnv.registerTable("T1", t1);

		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds2 = StreamTestData.get5TupleDataStream(env);
		tableEnv.registerDataStream("T2", ds2, "a, b, d, c, e");

		String sqlQuery = "SELECT STREAM * FROM T1 " +
							"UNION ALL " +
							"(SELECT STREAM a, b, c FROM T2 WHERE a	< 3)";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList();
		expected.add("1,1,Hi");
		expected.add("2,2,Hello");
		expected.add("3,2,Hello world");
		expected.add("1,1,Hallo");
		expected.add("2,2,Hallo Welt");
		expected.add("2,3,Hallo Welt wie");

		StreamITCase.compareWithList(expected);
	}
}
