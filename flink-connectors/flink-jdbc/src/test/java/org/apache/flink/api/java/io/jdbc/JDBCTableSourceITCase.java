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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.types.Row;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * IT case for {@link JDBCTableSource}.
 */
public class JDBCTableSourceITCase extends JDBCTestBase {

	private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	private static final EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
	private static final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

	static final String TABLE_SOURCE_SQL = "CREATE TABLE books (" +
		" id int, " +
		" title varchar, " +
		" author varchar, " +
		" price double, " +
		" qty int " +
		") with (" +
		" 'connector.type' = 'jdbc', " +
		" 'connector.url' = 'jdbc:derby:memory:ebookshop', " +
		" 'connector.table' = 'books', " +
		" 'connector.driver' = 'org.apache.derby.jdbc.EmbeddedDriver' " +
		")";

	@BeforeClass
	public static void createTable() {
		tEnv.sqlUpdate(TABLE_SOURCE_SQL);
	}

	@Test
	public void testFieldsProjection() throws Exception {
		StreamITCase.clear();

		Table result = tEnv.sqlQuery(SELECT_ID_BOOKS);
		DataStream<Row> resultSet = tEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1001");
		expected.add("1002");
		expected.add("1003");
		expected.add("1004");
		expected.add("1005");
		expected.add("1006");
		expected.add("1007");
		expected.add("1008");
		expected.add("1009");
		expected.add("1010");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testAllFieldsSelection() throws Exception {
		StreamITCase.clear();

		Table result = tEnv.sqlQuery(SELECT_ALL_BOOKS);
		DataStream<Row> resultSet = tEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1001,Java public for dummies,Tan Ah Teck,11.11,11");
		expected.add("1002,More Java for dummies,Tan Ah Teck,22.22,22");
		expected.add("1003,More Java for more dummies,Mohammad Ali,33.33,33");
		expected.add("1004,A Cup of Java,Kumar,44.44,44");
		expected.add("1005,A Teaspoon of Java,Kevin Jones,55.55,55");
		expected.add("1006,A Teaspoon of Java 1.4,Kevin Jones,66.66,66");
		expected.add("1007,A Teaspoon of Java 1.5,Kevin Jones,77.77,77");
		expected.add("1008,A Teaspoon of Java 1.6,Kevin Jones,88.88,88");
		expected.add("1009,A Teaspoon of Java 1.7,Kevin Jones,99.99,99");
		expected.add("1010,A Teaspoon of Java 1.8,Kevin Jones,null,1010");

		StreamITCase.compareWithList(expected);
	}

}
