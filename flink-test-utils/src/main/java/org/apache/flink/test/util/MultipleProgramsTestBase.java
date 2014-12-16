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
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

public class MultipleProgramsTestBase extends TestBaseUtils {
	protected static ForkableFlinkMiniCluster cluster = null;
	protected transient ExecutionMode mode;

	public MultipleProgramsTestBase(ExecutionMode mode){
		this.mode = mode;
		switch(mode){
			case CLUSTER:
				TestEnvironment clusterEnv = new TestEnvironment(cluster, 4);
				clusterEnv.setAsContext();
				break;
			case COLLECTION:
				CollectionTestEnvironment collectionEnv = new CollectionTestEnvironment();
				collectionEnv.setAsContext();
				break;
		}
	}

	@BeforeClass
	public static void setup() throws Exception{
		cluster = TestBaseUtils.startCluster(1, 4);
	}

	@AfterClass
	public static void teardown() throws Exception {
		stopCluster(cluster, TestBaseUtils.DEFAULT_TIMEOUT);
	}

	@Parameterized.Parameters(name = "Execution mode = {0}")
	public static Collection<ExecutionMode[]> executionModes(){
		return Arrays.asList(new ExecutionMode[]{ExecutionMode.CLUSTER},
				new ExecutionMode[]{ExecutionMode.COLLECTION});
	}

	protected static enum ExecutionMode{
		CLUSTER, COLLECTION
	}
}
