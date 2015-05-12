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

package org.apache.flink.streaming.util;

import org.apache.flink.test.util.AbstractMultipleProgramsTestBase;
import org.apache.flink.test.util.AbstractMultipleProgramsTestBase.TestExecutionMode;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Base class for streaming unit tests that run multiple tests and want to reuse the same
 * Flink cluster. This saves a significant amount of time, since the startup and
 * shutdown of the Flink clusters (including actor systems, etc) usually dominates
 * the execution of the actual tests.
 *
 * To write a unit test against this test base, simply extend it and add
 * one or more regular test methods and retrieve the StreamExecutionEnvironment from
 * the context:
 *
 * <pre>{@code
 *
 *   @Test
 *   public void someTest() {
 *       StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *       // test code
 *       env.execute();
 *   }
 *
 *   @Test
 *   public void anotherTest() {
 *       StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *       // test code
 *       env.execute();
 *   }
 *
 * }</pre>
 */
public class StreamingMultipleProgramsTestBase extends AbstractMultipleProgramsTestBase {

	public StreamingMultipleProgramsTestBase(TestExecutionMode mode) {
		super(mode);
		switch(this.mode){
			case CLUSTER:
				TestStreamEnvironment clusterEnv = new TestStreamEnvironment(cluster, 4);
				clusterEnv.setAsContext();
				break;
			case COLLECTION:
				throw new UnsupportedOperationException("Flink streaming currently has no collection execution backend.");
		}
	}

	@Parameterized.Parameters(name = "Execution mode = {0}")
	public static Collection<TestExecutionMode[]> executionModes() {
		TestExecutionMode[] tems = new TestExecutionMode[]{TestExecutionMode.CLUSTER};
		ArrayList<TestExecutionMode[]> temsList = new ArrayList<TestExecutionMode[]>();
		temsList.add(tems);
		return temsList;
	}
}
