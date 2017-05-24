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

import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Base class for unit tests that run multiple tests and want to reuse the same
 * Flink cluster. This saves a significant amount of time, since the startup and
 * shutdown of the Flink clusters (including actor systems, etc) usually dominates
 * the execution of the actual tests.
 *
 * <p>To write a unit test against this test base, simply extend it and add
 * one or more regular test methods and retrieve the ExecutionEnvironment from
 * the context:
 *
 * <pre>{@code
 *
 *   {@literal @}Test
 *   public void someTest() {
 *       ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
 *       // test code
 *       env.execute();
 *   }
 *
 *   {@literal @}Test
 *   public void anotherTest() {
 *       ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
 *       // test code
 *       env.execute();
 *   }
 *
 * }</pre>
 */
public class MultipleProgramsTestBase extends TestBaseUtils {

	/**
	 * Enum that defines which execution environment to run the next test on:
	 * An embedded local flink cluster, or the collection execution backend.
	 */
	public enum TestExecutionMode {
		CLUSTER,
		CLUSTER_OBJECT_REUSE,
		COLLECTION,
	}

	// ------------------------------------------------------------------------
	//  The mini cluster that is shared across tests
	// ------------------------------------------------------------------------

	protected static final int DEFAULT_PARALLELISM = 4;

	protected static boolean startWebServer = false;

	protected static LocalFlinkMiniCluster cluster = null;

	// ------------------------------------------------------------------------

	protected final TestExecutionMode mode;

	public MultipleProgramsTestBase(TestExecutionMode mode) {
		this.mode = mode;
	}

	// ------------------------------------------------------------------------
	//  Environment setup & teardown
	// ------------------------------------------------------------------------

	@Before
	public void setupEnvironment() {
		switch(mode){
			case CLUSTER:
				new TestEnvironment(cluster, 4, false).setAsContext();
				break;
			case CLUSTER_OBJECT_REUSE:
				new TestEnvironment(cluster, 4, true).setAsContext();
				break;
			case COLLECTION:
				new CollectionTestEnvironment().setAsContext();
				break;
		}
	}

	@After
	public void teardownEnvironment() {
		switch(mode) {
			case CLUSTER:
			case CLUSTER_OBJECT_REUSE:
				TestEnvironment.unsetAsContext();
				break;
			case COLLECTION:
				CollectionTestEnvironment.unsetAsContext();
				break;
		}
	}

	// ------------------------------------------------------------------------
	//  Cluster setup & teardown
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void setup() throws Exception {
		cluster = TestBaseUtils.startCluster(
			1,
			DEFAULT_PARALLELISM,
			startWebServer,
			false,
			true);
	}

	@AfterClass
	public static void teardown() throws Exception {
		stopCluster(cluster, TestBaseUtils.DEFAULT_TIMEOUT);
	}

	// ------------------------------------------------------------------------
	//  Parametrization lets the tests run in cluster and collection mode
	// ------------------------------------------------------------------------

	@Parameterized.Parameters(name = "Execution mode = {0}")
	public static Collection<Object[]> executionModes() {
		return Arrays.asList(
				new Object[] { TestExecutionMode.CLUSTER },
				new Object[] { TestExecutionMode.COLLECTION });
	}
}
