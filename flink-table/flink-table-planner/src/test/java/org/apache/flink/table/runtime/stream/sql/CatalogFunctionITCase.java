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

package org.apache.flink.table.runtime.stream.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionTestBase;

import org.junit.BeforeClass;

/**
 * Tests for {@link CatalogFunction} in stream table environment.
 */
public class CatalogFunctionITCase extends CatalogFunctionTestBase {
	private static StreamExecutionEnvironment streamExecEnvironment;

	@BeforeClass
	public static void setup() {
		streamExecEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecEnvironment);
		setTableEnv(streamTableEnvironment);
	}

	@Override
	public void execute() throws Exception {
		streamExecEnvironment.execute();
	}
}
