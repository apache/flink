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

package org.apache.flink.table.api.java.stream.sql;

import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.scala.stream.utils.StreamITCase;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.table.api.java.stream.utils.StreamTestData;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class StreamAggregationSqlITCase extends StreamingMultipleProgramsTestBase {

	@Test
	public void testProcTime() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = StreamTestData.get5TupleDataStream(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT a, procTime() FROM MyTable ";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

	}
	
	@Ignore("not ready yet") @Test
	public void testMaxAggregatation() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = StreamTestData.get5TupleDataStream(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT a, MAX(c) OVER (PARTITION BY b ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS maxC FROM MyTable";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("5,14");

		StreamITCase.compareWithList(expected);
	}

}
