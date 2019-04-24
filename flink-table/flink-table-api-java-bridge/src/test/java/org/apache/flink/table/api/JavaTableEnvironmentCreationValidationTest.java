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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test failures for the creation of java {@link TableEnvironment}s.
 */
public class JavaTableEnvironmentCreationValidationTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void testBatchTableEnvironment() {
		exception.expect(TableException.class);
		exception.expectMessage("Create BatchTableEnvironment failed.");
		BatchTableEnvironment.create(ExecutionEnvironment.getExecutionEnvironment());
	}

	@Test
	public void testStreamTableEnvironment() {
		exception.expect(TableException.class);
		exception.expectMessage("Create StreamTableEnvironment failed.");
		StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
	}
}
