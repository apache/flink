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

public class ProcTimeRowStreamAggregationSqlITCase extends StreamingMultipleProgramsTestBase {

	
	@Test
	public void testMaxAggregatation() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		env.setParallelism(1);
		
		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = StreamTestData.get5TupleDataStream(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT a, MAX(c) OVER (PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS maxC FROM MyTable";
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
		
		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = StreamTestData.get5TupleDataStream(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT a, MIN(c) OVER (PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS maxC FROM MyTable";
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
		expected.add("4,8");
		expected.add("5,10");
		expected.add("5,10");
		expected.add("5,11");
		expected.add("5,12");
		expected.add("5,13");

		StreamITCase.compareWithList(expected);
	}
	
	@Test
	public void testSumAggregatation() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		env.setParallelism(1);
		
		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = StreamTestData.get5TupleDataStream(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT a, SUM(c) OVER (PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sumC FROM MyTable";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,0");
		expected.add("2,1");
		expected.add("2,3");
		expected.add("3,3");
		expected.add("3,7");
		expected.add("3,9");
		expected.add("4,6");
		expected.add("4,13");
		expected.add("4,15");
		expected.add("4,17");
		expected.add("5,10");
		expected.add("5,21");
		expected.add("5,23");
		expected.add("5,26");
		expected.add("5,27");

		StreamITCase.compareWithList(expected);
	}
	
	@Test
	public void testSumMinAggregatation() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		env.setParallelism(1);
		
		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = StreamTestData.get5TupleDataStream(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT a, "
				+ "SUM(c) OVER (PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sumC,"
				+ "MIN(c) OVER (PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sumC"
				+ " FROM MyTable";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,0,0"); 
		expected.add("2,1,1"); 
		expected.add("2,3,1"); 
		expected.add("3,3,3"); 
		expected.add("3,7,3"); 
		expected.add("3,9,4"); 
		expected.add("4,6,6"); 
		expected.add("4,13,6"); 
		expected.add("4,15,7"); 
		expected.add("4,17,8"); 
		expected.add("5,10,10");
		expected.add("5,21,10");
		expected.add("5,23,11");
		expected.add("5,26,12");
		expected.add("5,27,13");

		StreamITCase.compareWithList(expected);
	}
	
	@Test
	public void testGlobalSumAggregatation() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		env.setParallelism(1);
		
		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = StreamTestData.get5TupleDataStream(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT SUM(c) OVER (ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sumC FROM MyTable";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("0");
		expected.add("1");
		expected.add("3");
		expected.add("5");
		expected.add("7");
		expected.add("9");
		expected.add("11");
		expected.add("13");
		expected.add("15");
		expected.add("17");
		expected.add("19");
		expected.add("21");
		expected.add("23");
		expected.add("26");
		expected.add("27");

		StreamITCase.compareWithList(expected);
	}
	
	@Test
	public void testAvgAggregatation() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		env.setParallelism(1);
		
		DataStream<Tuple5<Integer, Long, Integer, String, Long>> ds = StreamTestData.get5TupleDataStream(env);
		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e");
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT a, AVG(c) OVER (PARTITION BY a ORDER BY procTime() ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avgC FROM MyTable";
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
		expected.add("4,8");
		expected.add("5,10");
		expected.add("5,10");
		expected.add("5,11");
		expected.add("5,13");
		expected.add("5,13");

		StreamITCase.compareWithList(expected);
	}
	

}
