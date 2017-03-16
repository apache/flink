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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.scala.stream.utils.StreamITCase;
import org.apache.flink.types.Row;
import org.junit.Test;

public class StreamProcTimeTimeBoundAggregationIntegrationTest extends StreamingMultipleProgramsTestBase {



	
//	@Ignore("not ready yet") 
	@Test
	public void testMaxAggregatation() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		env.setParallelism(1);
		
		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = get5TupleDataStream(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT a, MAX(c) OVER (PARTITION BY a ORDER BY procTime() RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS maxC FROM MyTable";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,0");
		expected.add("2,1");
		expected.add("2,2");
		expected.add("3,3");
		expected.add("3,4");
		expected.add("3,5");
		expected.add("4,6");
		expected.add("4,7");
		expected.add("4,8");
		expected.add("4,9");
		expected.add("5,10");
		expected.add("5,11");
		expected.add("5,12");
		expected.add("5,14");
		expected.add("5,14");

		StreamITCase.compareWithList(expected);
	}
	
	@Test
	public void testMinAggregatation() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		env.setParallelism(1);
		
		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = get5TupleDataStream(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT a, MIN(c) OVER (PARTITION BY a ORDER BY procTime() RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS maxC FROM MyTable";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,0");
		expected.add("2,1");
		expected.add("2,1");
		expected.add("3,3");
		expected.add("3,3");
		expected.add("3,3");
		expected.add("4,6");
		expected.add("4,6");
		expected.add("4,6");
		expected.add("4,6");
		expected.add("5,10");
		expected.add("5,10");
		expected.add("5,10");
		expected.add("5,10");
		expected.add("5,10");

		StreamITCase.compareWithList(expected);
	}
	
	@Test
	public void testSumAggregatation() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		env.setParallelism(1);
		
		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = get5TupleDataStream(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT a, SUM(c) OVER (PARTITION BY a ORDER BY procTime() RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS sumC FROM MyTable";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,0");
		expected.add("2,1");
		expected.add("2,3");
		expected.add("3,12");
		expected.add("3,3");
		expected.add("3,7");
		expected.add("4,13");
		expected.add("4,21");
		expected.add("4,30");
		expected.add("4,6");
		expected.add("5,10");
		expected.add("5,21");
		expected.add("5,33");
		expected.add("5,47");
		expected.add("5,60");

		StreamITCase.compareWithList(expected);
	}
	
	@Test
	public void testAvgAggregatation() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		env.setParallelism(1);
		
		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = get5TupleDataStream(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT a, AVG(c) OVER (PARTITION BY a ORDER BY procTime() RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS avgC FROM MyTable";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,0");
		expected.add("2,1");
		expected.add("2,1");
		expected.add("3,3");
		expected.add("3,3");
		expected.add("3,4");
		expected.add("4,6");
		expected.add("4,6");
		expected.add("4,7");
		expected.add("4,7");
		expected.add("5,10");
		expected.add("5,10");
		expected.add("5,11");
		expected.add("5,11");
		expected.add("5,12");

		StreamITCase.compareWithList(expected);
	}
	
	public DataStream<Tuple5<Integer, Long, Integer, String, Long>> get5TupleDataStream(StreamExecutionEnvironment env) {

		List<Tuple5<Integer, Long, Integer, String, Long>> data = new ArrayList<>();
		data.add(new Tuple5<>(1, 1L, 0, "Hallo", 1L));
		data.add(new Tuple5<>(2, 2L, 1, "Hallo Welt", 2L));
		data.add(new Tuple5<>(2, 3L, 2, "Hallo Welt wie", 1L));
		data.add(new Tuple5<>(3, 4L, 3, "Hallo Welt wie gehts?", 2L));
		data.add(new Tuple5<>(3, 5L, 4, "ABC", 2L));
		data.add(new Tuple5<>(3, 6L, 5, "BCD", 3L));
		data.add(new Tuple5<>(4, 7L, 6, "CDE", 2L));
		data.add(new Tuple5<>(4, 8L, 7, "DEF", 1L));
		data.add(new Tuple5<>(4, 9L, 8, "EFG", 1L));
		data.add(new Tuple5<>(4, 10L, 9, "FGH", 2L));
		data.add(new Tuple5<>(5, 11L, 10, "GHI", 1L));
		data.add(new Tuple5<>(5, 12L, 11, "HIJ", 3L));
		data.add(new Tuple5<>(5, 13L, 12, "IJK", 3L));
		data.add(new Tuple5<>(5, 15L, 14, "KLM", 2L));
		data.add(new Tuple5<>(5, 14L, 13, "JKL", 2L));
		return env.fromCollection(data);
	}

}
