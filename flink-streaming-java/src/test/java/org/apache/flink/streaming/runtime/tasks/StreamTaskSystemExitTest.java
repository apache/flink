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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.UserSystemExitException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.security.FlinkUserSecurityManager;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

/**
 * Tests for stream task where user-invokable codes try to exit JVM.
 * Currently, monitoring system exit is enabled inside relevant methods that can call user-defined
 * functions in {@code StreamTask}.
 */
public class StreamTaskSystemExitTest {
	private static final int TEST_EXIT_CODE = 123;
	private FlinkUserSecurityManager flinkUserSecurityManager;

	@Before
	public void setUp() {
		flinkUserSecurityManager = new FlinkUserSecurityManager(FlinkUserSecurityManager.CheckExitMode.THROW);
		System.setSecurityManager(flinkUserSecurityManager);
	}

	@After
	public void tearDown() {
		assertFalse(flinkUserSecurityManager.systemExitMonitored());
		System.setSecurityManager(flinkUserSecurityManager.getOriginalSecurityManager());
	}

	@Test(expected = UserSystemExitException.class)
	public void testInitSystemExitStreamTask() throws Exception {
		Environment mockEnvironment = new MockEnvironmentBuilder().build();
		SystemExitStreamTask systemExitStreamTask = new SystemExitStreamTask(mockEnvironment, SystemExitStreamTask.ExitPoint.INIT);
		systemExitStreamTask.invoke();
	}

	@Test(expected = UserSystemExitException.class)
	public void testProcessInputSystemExitStreamTask() throws Exception {
		Environment mockEnvironment = new MockEnvironmentBuilder().build();
		SystemExitStreamTask systemExitStreamTask = new SystemExitStreamTask(mockEnvironment, SystemExitStreamTask.ExitPoint.PROCESS_INPUT);
		systemExitStreamTask.invoke();
	}

	@Test(expected = UserSystemExitException.class)
	public void testCancelSystemExitStreamTask() throws Exception {
		Environment mockEnvironment = new MockEnvironmentBuilder().build();
		SystemExitStreamTask systemExitStreamTask = new SystemExitStreamTask(mockEnvironment);
		systemExitStreamTask.cancel();
	}

	private static class SystemExitStreamTask extends StreamTask<String, AbstractStreamOperator<String>> {
		/**
		 * Inside invoke() call, specify where system exit is called.
		 */
		private enum ExitPoint {
			NONE,
			INIT,
			PROCESS_INPUT,
		}

		private final ExitPoint exitPoint;

		public SystemExitStreamTask(Environment env) throws Exception {
			this(env, ExitPoint.NONE);
		}

		public SystemExitStreamTask(Environment env, ExitPoint exitPoint) throws Exception {
			super(env, null);
			this.exitPoint = exitPoint;
		}

		@Override
		protected void init() {
			if (exitPoint == ExitPoint.INIT) {
				systemExit();
			}
		}

		@Override
		protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
			if (exitPoint == ExitPoint.PROCESS_INPUT) {
				systemExit();
			}
		}

		@Override
		protected void cleanup() {}

		@Override
		protected void cancelTask() {
			systemExit();
		}

		/**
		 * Perform the check System.exit() does through security manager without actually calling System.exit() not to
		 * confuse Junit about failed test.
		 */
		private void systemExit() {
			SecurityManager securityManager = System.getSecurityManager();
			if (securityManager != null) {
				securityManager.checkExit(TEST_EXIT_CODE);
			}
		}
	}
}
