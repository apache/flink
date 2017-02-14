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

import org.apache.flink.api.java.tuple.Tuple4;
import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.scala.stream.utils.StreamITCase;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class WindowAggregateITCase extends StreamingMultipleProgramsTestBase {
	private static final List<Tuple4<Timestamp, Integer, String, Long>>
		DATA = ImmutableList.of(
		new Tuple4<>(new Timestamp(1485914055000L), 1, "foo", 2L),
		new Tuple4<>(new Timestamp(1485914056000L), 2, "bar", 3L),
		new Tuple4<>(new Timestamp(1485914056000L), 2, "foo", 4L),
		new Tuple4<>(new Timestamp(1485934057000L), 3, "bar", 5L)
	);

	private static final AscendingTimestampExtractor<Tuple4<Timestamp, Integer, String, Long>>
		WATERMARKS = new AscendingTimestampExtractor<Tuple4<Timestamp, Integer, String, Long>>() {
		@Override
		public long extractAscendingTimestamp(Tuple4<Timestamp, Integer, String, Long> e) {
			return e.f0.getTime();
		}
	};

	private static Table getTable(StreamTableEnvironment tableEnv) {
		DataStream<Tuple4<Timestamp, Integer, String, Long>> ds = tableEnv.execEnv().fromCollection(DATA)
			.assignTimestampsAndWatermarks(WATERMARKS);
		return tableEnv.fromDataStream(ds, "ts,quantity,id,value");
	}

	@Before
	public void setUp() {
		StreamITCase.clear();
	}

	@Test
	public void testTumbleWindow() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		Table in = getTable(tableEnv);
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT COUNT(*) FROM MyTable GROUP BY FLOOR(rowtime() TO HOUR)";
		Table result = tableEnv.sql(sqlQuery);
		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("3");
		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testMultiGroup() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		Table in = getTable(tableEnv);
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT id, COUNT(*) FROM MyTable GROUP BY FLOOR(rowtime() TO HOUR), id";
		Table result = tableEnv.sql(sqlQuery);
		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("bar,1");
		expected.add("foo,2");
		StreamITCase.compareWithList(expected);
	}

	@Test(expected = TableException.class)
	public void testMultiWindow() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		Table in = getTable(tableEnv);
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT COUNT(*) FROM MyTable GROUP BY FLOOR(rowtime() TO HOUR), FLOOR(rowtime() TO MINUTE)";
		Table result = tableEnv.sql(sqlQuery);
		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();
	}

	@Test(expected = TableException.class)
	public void testInvalidWindowExpression() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		Table in = getTable(tableEnv);
		tableEnv.registerTable("MyTable", in);

		String sqlQuery = "SELECT COUNT(*) FROM MyTable GROUP BY FLOOR(localTimestamp TO HOUR)";
		Table result = tableEnv.sql(sqlQuery);
		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
		env.execute();
	}
}
