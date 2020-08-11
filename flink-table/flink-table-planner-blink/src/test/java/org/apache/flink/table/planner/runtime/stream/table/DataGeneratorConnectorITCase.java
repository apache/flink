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

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;

import org.junit.Test;

public class DataGeneratorConnectorITCase extends StreamingTestBase {

	private static final String TABLE = "CREATE TABLE datagen_t (\n" +
		"	f0 CHAR(1),\n" +
		"	f1 VARCHAR(10),\n" +
		"	f2 STRING,\n" +
		"	f3 BOOLEAN,\n" +
		"	f4 DECIMAL(32,2),\n" +
		"	f5 TINYINT,\n" +
		"	f6 SMALLINT,\n" +
		"	f7 INT,\n" +
		"	f8 BIGINT,\n" +
		"	f9 FLOAT,\n" +
		"	f10 DOUBLE,\n" +
		"	f11 DATE,\n" +
		"	f12 TIME,\n" +
		"	f13 TIMESTAMP(3),\n" +
		"	f14 TIMESTAMP WITH LOCAL TIME ZONE,\n" +
		"	f15 INT ARRAY,\n" +
		"	f16 MAP<STRING, DATE>,\n" +
		"	f17 DECIMAL(32,2) MULTISET,\n" +
		"	f18 ROW<a BIGINT, b TIME, c ROW<d TIMESTAMP>>\n" +
		") WITH (" +
		"	'connector' = 'datagen',\n" +
		"	'number-of-rows' = '10'\n" +
		")";

	private static final String SINK = "CREATE TABLE sink WITH ('connector' = 'blackhole') LIKE datagen_t (EXCLUDING ALL)";

	@Test
	public void testTypes() {
		tEnv().executeSql(TABLE);
		tEnv().executeSql(SINK);
		tEnv().from("datagen_t").executeInsert("sink");

		execInsertTableAndWaitResult(
			tEnv().from("datagen_t"),
			"sink");
	}
}
