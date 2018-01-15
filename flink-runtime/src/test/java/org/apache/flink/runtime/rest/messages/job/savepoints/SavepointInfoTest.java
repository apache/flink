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

package org.apache.flink.runtime.rest.messages.job.savepoints;

import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Tests for {@link SavepointInfo}.
 */
public class SavepointInfoTest extends TestLogger {

	@Test
	public void testNullRequestId() {
		try {
			new SavepointInfo(null, "/tmp", null);
			fail("Expected exception not thrown");
		} catch (NullPointerException e) {
		}
	}

	@Test
	public void testSetBothLocationAndFailureCause()  {
		try {
			new SavepointInfo(
				new SavepointTriggerId(),
				"/tmp",
				new SerializedThrowable(new RuntimeException()));
			fail("Expected exception not thrown");
		} catch (IllegalArgumentException e) {
		}
	}
}
