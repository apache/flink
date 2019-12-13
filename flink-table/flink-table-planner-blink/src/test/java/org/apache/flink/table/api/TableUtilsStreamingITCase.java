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

package org.apache.flink.table.api;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * IT case for {@link TableUtils} in streaming mode.
 */
public class TableUtilsStreamingITCase {

	private StreamExecutionEnvironment env;
	private StreamTableEnvironment tEnv;

	@Before
	public void before() {
		env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(4);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		tEnv = StreamTableEnvironment.create(env, settings);
	}

	@Test
	public void testCollectToListForAppendStream() throws Exception {
		List<Row> sourceData = Arrays.asList(
			Row.of(1, 11L),
			Row.of(1, 12L),
			Row.of(2, 21L),
			Row.of(2, 22L),
			Row.of(3, 31L));
		tEnv.registerTable("T", tEnv.fromDataStream(env.fromCollection(sourceData), "a, b"));

		String sql = "SELECT b FROM T WHERE a NOT IN (1, 2, 4, 5)";
		List<Row> expected = Collections.singletonList(Row.of(31L));
		Table table = tEnv.sqlQuery(sql);
		// run multiple times to make sure no errors will occur
		// when the utility method is called a second time
		for (int i = 0; i < 2; i++) {
			List<Row> actual = TableUtils.collectToList(table);
			assertEquals(expected, actual);
		}
	}
}
