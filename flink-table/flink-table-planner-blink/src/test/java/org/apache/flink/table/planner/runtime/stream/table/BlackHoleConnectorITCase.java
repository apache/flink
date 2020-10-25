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

import org.apache.flink.table.factories.BlackHoleTableSinkFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;

import org.junit.Test;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.row;

/**
 * End to end tests for {@link BlackHoleTableSinkFactory}.
 */
public class BlackHoleConnectorITCase extends StreamingTestBase {

	@Test
	public void testTypes() throws Exception {
		tEnv().executeSql(
				"create table blackhole_t (f0 int, f1 double) with ('connector' = 'blackhole')");
		tEnv().fromValues(Arrays.asList(row(1, 1.1), row(2, 2.2))).executeInsert("blackhole_t").await();
	}
}
