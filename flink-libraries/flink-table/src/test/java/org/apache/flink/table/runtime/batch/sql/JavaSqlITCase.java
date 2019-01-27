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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConverters;

import static org.apache.flink.test.util.TestBaseUtils.compareResultAsText;

/**
 * Integration tests for batch SQL.
 */
public class JavaSqlITCase extends BatchTestBase {

	public static List<Row> getResult(Table table) {
		return JavaConverters.seqAsJavaListConverter(table.collect()).asJava();
	}

	@Test
	public void testValues() throws Exception {

		String sqlQuery = "VALUES (1, 'Test', TRUE, DATE '1944-02-24', 12.4444444444444445)," +
			"(2, 'Hello', TRUE, DATE '1944-02-24', 12.666666665)," +
			"(3, 'World', FALSE, DATE '1944-12-24', 12.54444445)";
		Table result = tEnv().sqlQuery(sqlQuery);

		String expected = "3,World,false,1944-12-24,12.5444444500000000\n" +
			"2,Hello,true,1944-02-24,12.6666666650000000\n" +
			// Calcite converts to decimals and strings with equal length
			"1,Test,true,1944-02-24,12.4444444444444445\n";
		compareResultAsText(getResult(result), expected);
	}

	@Test
	public void testSelectFromTable() throws Exception {
		registerCollection("T", TestData.data3(), TestData.type3(), "a, b, c");

		String sqlQuery = "SELECT a, c FROM T";
		Table result = tEnv().sqlQuery(sqlQuery);

		String expected = "1,Hi\n" + "2,Hello\n" + "3,Hello world\n" +
			"4,Hello world, how are you?\n" + "5,I am fine.\n" + "6,Luke Skywalker\n" +
			"7,Comment#1\n" + "8,Comment#2\n" + "9,Comment#3\n" + "10,Comment#4\n" +
			"11,Comment#5\n" + "12,Comment#6\n" + "13,Comment#7\n" +
			"14,Comment#8\n" + "15,Comment#9\n" + "16,Comment#10\n" +
			"17,Comment#11\n" + "18,Comment#12\n" + "19,Comment#13\n" +
			"20,Comment#14\n" + "21,Comment#15\n";
		compareResultAsText(getResult(result), expected);
	}

	@Test
	public void testFilterFromDataSet() throws Exception {

		registerCollection("T", TestData.data3(), TestData.type3(), "x, y, z");

		String sqlQuery = "SELECT x FROM T WHERE z LIKE '%Hello%'";
		Table result = tEnv().sqlQuery(sqlQuery);

		String expected = "2\n" + "3\n" + "4";
		compareResultAsText(getResult(result), expected);
	}

	@Test
	public void testAggregation() throws Exception {

		registerCollection("AggTable", TestData.data3(), TestData.type3(), "x, y, z");

		String sqlQuery = "SELECT sum(x), min(x), max(x), count(y), avg(x) FROM AggTable";
		Table result = tEnv().sqlQuery(sqlQuery);

		String expected = "231,1,21,21,11.0";
		compareResultAsText(getResult(result), expected);
	}

	@Test
	public void testJoin() throws Exception {
		registerCollection("t1", TestData.smallData3(), TestData.type3(), "a, b, c");
		registerCollection("t2", TestData.data5(), TestData.type5(), "d, e, f, g, h");

		String sqlQuery = "SELECT c, g FROM t1, t2 WHERE b = e";
		Table result = tEnv().sqlQuery(sqlQuery);

		String expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n";
		compareResultAsText(getResult(result), expected);
	}

	@Test
	public void testMap() throws Exception {
		List<Tuple2<Integer, Map<String, String>>> rows = new ArrayList<>();
		rows.add(new Tuple2<>(1, Collections.singletonMap("foo", "bar")));
		rows.add(new Tuple2<>(2, Collections.singletonMap("foo", "spam")));

		TypeInformation<Tuple2<Integer, Map<String, String>>> ty = new TupleTypeInfo<>(
			BasicTypeInfo.INT_TYPE_INFO,
			new MapTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

		registerJavaCollection("t1", rows, ty, "a, b");

		String sqlQuery = "SELECT b['foo'] FROM t1";
		Table result = tEnv().sqlQuery(sqlQuery);

		String expected = "bar\n" + "spam\n";
		compareResultAsText(getResult(result), expected);
	}

	@Test
	public void testPojo() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4);
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env);
		DataStreamSource<WC> input = env.fromElements(
				new WC("Hello", 1, "xx1"),
				new WC("Ciao", 1, "xx2"),
				new WC("Hello", 1, "xx3"));

		// register the BoundedStream as table "WordCount"
		tEnv.registerBoundedStream("WC", input, "word, frequency, amount");

		// run a SQL query on the Table and retrieve the result as a new Table
		Table result = tEnv.sqlQuery(
				"SELECT word, frequency, amount FROM WC");

		String expected = "Ciao,1,xx2\n" + "Hello,1,xx1\n" + "Hello,1,xx3\n";
		compareResultAsText(getResult(result), expected);
	}

	/**
	 * Simple POJO containing a word and its respective count.
	 */
	public static class WC {
		public String word;
		public long frequency;
		public String amount;

		// public constructor to make it a Flink POJO
		public WC() {}

		public WC(String word, long frequency, String amount) {
			this.word = word;
			this.frequency = frequency;
			this.amount = amount;
		}

		@Override
		public String toString() {
			return "WC " + word + " " + frequency + " " + amount;
		}
	}
}
