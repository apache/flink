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

import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

/**
 * Base class for unit tests that run a single test.
 *
 * <p>To write a unit test against this test base, simply extend it and implement the {@link #testProgram()} method.
 */
public abstract class StreamingProgramTestBase extends AbstractTestBase {

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
		// pre-submit
		preSubmit();

		// call the test program
		testProgram();

		// post-submit
		postSubmit();
	}
}
