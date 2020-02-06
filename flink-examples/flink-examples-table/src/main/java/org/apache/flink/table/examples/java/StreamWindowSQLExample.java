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

package org.apache.flink.table.examples.java;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import java.io.File;

/**
 * Simple example for demonstrating the use of SQL in Java.
 *
 * <p>This example shows how to:
 *  - Register a table via DDL
 *  - Declare an event time attribute in the DDL
 *  - Run a streaming window aggregate on the registered table
 */
public class StreamWindowSQLExample {

	public static void main(String[] args) throws Exception {

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// use blink planner in streaming mode,
		// because watermark statement is only available in blink planner.
		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

		// write source data into temporary file and get the absolute path
		String contents = "1,beer,3,2019-12-12 00:00:01\n" +
			"1,diaper,4,2019-12-12 00:00:02\n" +
			"2,pen,3,2019-12-12 00:00:04\n" +
			"2,rubber,3,2019-12-12 00:00:06\n" +
			"3,rubber,2,2019-12-12 00:00:05\n" +
			"4,beer,1,2019-12-12 00:00:08";
		File tempFile = File.createTempFile("orders", ".csv");
		tempFile.deleteOnExit();
		FileUtils.writeFileUtf8(tempFile, contents);
		String path = tempFile.toURI().toString();
		System.out.println(path);

		// register table via DDL with watermark,
		// the events are out of order, hence, we use 3 seconds to wait the late events
		String ddl = "CREATE TABLE orders (\n" +
			"  user_id INT,\n" +
			"  product STRING,\n" +
			"  amount INT,\n" +
			"  ts TIMESTAMP(3),\n" +
			"  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND\n" +
			") WITH (\n" +
			"  'connector.type' = 'filesystem',\n" +
			"  'connector.path' = '" + path + "',\n" +
			"  'format.type' = 'csv'\n" +
			")";
		tEnv.sqlUpdate(ddl);

		// run a SQL query on the table and retrieve the result as a new Table
		String query = "SELECT\n" +
			"  CAST(TUMBLE_START(ts, INTERVAL '5' SECOND) AS STRING) window_start,\n" +
			"  COUNT(*) order_num,\n" +
			"  SUM(amount) total_amount,\n" +
			"  COUNT(DISTINCT product) unique_products\n" +
			"FROM orders\n" +
			"GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";
		Table result = tEnv.sqlQuery(query);
		tEnv.toAppendStream(result, Row.class).print();

		// submit the job
		tEnv.execute("Streaming Window SQL Job");

		// should output:
		// 2019-12-12 00:00:00.000,3,10,3
		// 2019-12-12 00:00:05.000,3,6,2
	}
}
