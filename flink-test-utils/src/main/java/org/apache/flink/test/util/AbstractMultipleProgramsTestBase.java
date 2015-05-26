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

package org.apache.flink.test.util;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Abstract base class for unit tests that run multiple tests and want to reuse the same
 * Flink cluster. This saves a significant amount of time, since the startup and
 * shutdown of the Flink clusters (including actor systems, etc) usually dominates
 * the execution of the actual tests.
 *
 * To write a unit test against this test base, simply extend it and add
 * one or more regular test methods and retrieve the ExecutionEnvironment from
 * the context:
 *
 * <pre>{@code
 *
 *   @Test
 *   public void someTest() {
 *       ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
 *       // test code
 *       env.execute();
 *   }
 *
 *   @Test
 *   public void anotherTest() {
 *       ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
 *       // test code
 *       env.execute();
 *   }
 *
 * }</pre>
 */
public abstract class AbstractMultipleProgramsTestBase extends TestBaseUtils {

	/**
	 * Enum that defines which execution environment to run the next test on:
	 * An embedded local flink cluster, or the collection execution backend.
	 */
	public enum TestExecutionMode {
		CLUSTER,
		COLLECTION
	}

	// -----------------------------------------------------------------------------------------...

	private static final int DEFAULT_PARALLELISM = 4;

	protected static ForkableFlinkMiniCluster cluster = null;

	protected transient TestExecutionMode mode;

	public static boolean singleActorSystem = true;

	public AbstractMultipleProgramsTestBase(TestExecutionMode mode){
		this.mode = mode;
	}

	@BeforeClass
	public static void setup() throws Exception{
		cluster = TestBaseUtils.startCluster(1, DEFAULT_PARALLELISM, false, singleActorSystem);
	}

	@AfterClass
	public static void teardown() throws Exception {
		stopCluster(cluster, TestBaseUtils.DEFAULT_TIMEOUT);
	}
}
