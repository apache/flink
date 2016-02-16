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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import static org.junit.Assert.fail;

public abstract class StreamingProgramTestBase extends AbstractTestBase {

	protected static final int DEFAULT_PARALLELISM = 4;

	private int parallelism;
	
	
	public StreamingProgramTestBase() {
		super(new Configuration());
		setParallelism(DEFAULT_PARALLELISM);
	}


	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
		setTaskManagerNumSlots(parallelism);
	}
	
	public int getParallelism() {
		return parallelism;
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
	public void testJob() throws Exception {
		try {
			// pre-submit
			try {
				preSubmit();
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				fail("Pre-submit work caused an error: " + e.getMessage());
			}

			// prepare the test environment
			startCluster();

			TestStreamEnvironment.setAsContext(this.executor, getParallelism());

			// call the test program
			try {
				testProgram();
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				fail("Error while calling the test program: " + e.getMessage());
			}
			finally {
				TestStreamEnvironment.unsetAsContext();
			}

			// post-submit
			try {
				postSubmit();
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
				fail("Post-submit work caused an error: " + e.getMessage());
			}
		}
		finally {
			stopCluster();
		}
	}
}
