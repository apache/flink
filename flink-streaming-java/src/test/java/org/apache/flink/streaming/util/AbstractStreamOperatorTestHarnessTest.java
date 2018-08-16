/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.containsString;

/**
 * Tests for {@link AbstractStreamOperatorTestHarness}.
 */
public class AbstractStreamOperatorTestHarnessTest extends TestLogger {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testInitializeAfterOpenning() throws Throwable {
		expectedException.expect(IllegalStateException.class);
		expectedException.expectMessage(containsString("TestHarness has already been initialized."));

		AbstractStreamOperatorTestHarness<Integer> result;
		result =
			new AbstractStreamOperatorTestHarness<>(
				new AbstractStreamOperator<Integer>() {
				},
				1,
				1,
				0);
		result.setup();
		result.open();
		result.initializeState(new OperatorSubtaskState());
	}
}
