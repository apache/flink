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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Assert;
import org.junit.Test;

public abstract class StreamingProgramTestBase extends AbstractTestBase {

	private static final int DEFAULT_PARALLELISM = 4;

	private TestStreamEnvironment env;

	private JobExecutionResult latestExecutionResult;

	private int parallelism = DEFAULT_PARALLELISM;


	public StreamingProgramTestBase() {
		this(new Configuration());
	}

	public StreamingProgramTestBase(Configuration config) {
		super(config);
		setTaskManagerNumSlots(parallelism);
	}
	
	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
		setTaskManagerNumSlots(parallelism);
	}
	
	public int getParallelism() {
		return parallelism;
	}
	
	public JobExecutionResult getLatestExecutionResult() {
		return this.latestExecutionResult;
	}
	

	// --------------------------------------------------------------------------------------------
	//  Methods to create the test program and for pre- and post- test work
	// --------------------------------------------------------------------------------------------

	protected abstract void testProgram() throws Exception;

	protected void preSubmit() throws Exception {}
	
	protected void postSubmit() throws Exception {}
	
	// --------------------------------------------------------------------------------------------
	//  Test entry point
	// --------------------------------------------------------------------------------------------

	@Test
	public void testJobWithoutObjectReuse() throws Exception {
		startCluster();
		try {
			// pre-submit
			try {
				preSubmit();
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Pre-submit work caused an error: " + e.getMessage());
			}

			// prepare the test environment
			env = new TestStreamEnvironment(this.executor, this.parallelism);
			env.setAsContext();

			// call the test program
			try {
				testProgram();
				this.latestExecutionResult = env.latestResult;
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Error while calling the test program: " + e.getMessage());
			}

			Assert.assertNotNull("The test program never triggered an execution.", this.latestExecutionResult);

			// post-submit
			try {
				postSubmit();
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				Assert.fail("Post-submit work caused an error: " + e.getMessage());
			}
		} finally {
			if(env.clusterRunsSynchronous()) {
				stopCluster();
			}
		}
	}

}
