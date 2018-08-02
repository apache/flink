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

package org.apache.flink.streaming.python.api;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.python.api.environment.PythonEnvironmentFactory;
import org.apache.flink.streaming.python.api.environment.PythonStreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link PythonStreamExecutionEnvironment}.
 */
public class PythonStreamExecutionEnvironmentTest extends AbstractTestBase {

	@ClassRule
	public static final TemporaryFolder TEMP_DIR = new TemporaryFolder();

	@Test
	public void testGetAndSetTimeCharacteristic() throws Exception {
		PythonEnvironmentFactory envFactory = new PythonEnvironmentFactory(TEMP_DIR.newFolder().getAbsolutePath(), "testPlan");
		PythonStreamExecutionEnvironment env = envFactory.get_execution_environment();

		env.set_stream_time_characteristic(TimeCharacteristic.EventTime);
		assertEquals(TimeCharacteristic.EventTime, env.get_stream_time_characteristic());

		env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime);
		assertEquals(TimeCharacteristic.ProcessingTime, env.get_stream_time_characteristic());
	}

}
