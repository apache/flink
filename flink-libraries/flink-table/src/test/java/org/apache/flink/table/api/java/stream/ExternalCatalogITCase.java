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

package org.apache.flink.table.api.java.stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.scala.stream.utils.StreamITCase;
import org.apache.flink.table.utils.CommonTestData;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ExternalCatalogITCase extends StreamingMultipleProgramsTestBase {

	@Test
	public void testSQL() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		tableEnv.registerExternalCatalog("test", CommonTestData.getInMemoryTestCatalog());

		String sqlQuery = "SELECT * FROM test.db1.tb1 " +
				"UNION ALL " +
				"(SELECT d, e, g FROM test.db2.tb2 WHERE d < 3)";
		Table result = tableEnv.sql(sqlQuery);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
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

	@Test
	public void testTableAPI() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		tableEnv.registerExternalCatalog("test", CommonTestData.getInMemoryTestCatalog());

		Table table1 = tableEnv.scan("test", "db1", "tb1");
		Table table2 = tableEnv.scan("test", "db2", "tb2");
		Table result = table2.where("d < 3").select("d, e, g").unionAll(table1);

		DataStream<Row> resultSet = tableEnv.toDataStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink());
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
