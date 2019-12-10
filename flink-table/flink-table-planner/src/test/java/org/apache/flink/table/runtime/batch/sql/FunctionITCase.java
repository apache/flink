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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.FunctionTestBase;

import org.junit.BeforeClass;

/**
 * Tests for catalog and system function in batch table environment.
 */
public class FunctionITCase extends FunctionTestBase {
	private static ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

	@BeforeClass
	public static void setup() {
		BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(executionEnvironment);
		setTableEnv(batchTableEnvironment);
	}

	@Override
	public void execute() throws Exception {
		executionEnvironment.execute();
	}
}
