/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph.restart;

import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link ThrowingRestartStrategy}.
 */
public class ThrowingRestartStrategyFactoryTest extends TestLogger {

	private RestartStrategy restartStrategy;

	@Before
	public void setUp() {
		restartStrategy = new ThrowingRestartStrategy();
	}

	@Test
	public void restartShouldThrowException() {
		final ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();

		try {
			restartStrategy.restart(new NoOpRestarter(), manuallyTriggeredScheduledExecutor);
			fail("Expected exception not thrown");
		} catch (IllegalStateException e) {
			assertThat(e.getMessage(), is(equalTo("Unexpected restart() call")));
			assertThat(manuallyTriggeredScheduledExecutor.numQueuedRunnables(), is(equalTo(0)));
		}
	}

	@Test
	public void canRestartShouldThrowException() {
		try {
			restartStrategy.canRestart();
			fail("Expected exception not thrown");
		} catch (IllegalStateException e) {
			assertThat(e.getMessage(), is(equalTo("Unexpected canRestart() call")));
		}
	}
}
