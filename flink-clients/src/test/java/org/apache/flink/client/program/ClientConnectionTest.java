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

package org.apache.flink.client.program;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.net.NetUtils;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * This test starts a job client without the JobManager being reachable. It
 * tests for a timely error and a meaningful error message.
 */
public class ClientConnectionTest {

	private static final long CONNECT_TIMEOUT = 2 * 1000; // 2 seconds
	private static final long ASK_STARTUP_TIMEOUT = 100 * 1000; // 100 seconds
	private static final long MAX_DELAY = 50 * 1000; // less than the startup timeout

	/**
	 * Tests the behavior against a LOCAL address where no job manager is running.
	 */
	@Test
	public void testExceptionWhenLocalJobManagerUnreachablelocal() {

		final InetSocketAddress unreachableEndpoint;
		try {
			int freePort = NetUtils.getAvailablePort();
			unreachableEndpoint = new InetSocketAddress(InetAddress.getLocalHost(), freePort);
		}
		catch (Throwable t) {
			// do not fail when we spuriously fail to get a free port
			return;
		}

		testFailureBehavior(unreachableEndpoint);
	}

	/**
	 * Tests the behavior against a REMOTE address where no job manager is running.
	 */
	@Test
	public void testExceptionWhenRemoteJobManagerUnreachable() {

		final InetSocketAddress unreachableEndpoint;
		try {
			int freePort = NetUtils.getAvailablePort();
			unreachableEndpoint = new InetSocketAddress(InetAddress.getByName("10.0.1.64"), freePort);
		}
		catch (Throwable t) {
			// do not fail when we spuriously fail to get a free port
			return;
		}

		testFailureBehavior(unreachableEndpoint);
	}

	private void testFailureBehavior(final InetSocketAddress unreachableEndpoint) {

		final Configuration config = new Configuration();
		config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, (ASK_STARTUP_TIMEOUT/1000) + " s");
		config.setString(ConfigConstants.AKKA_LOOKUP_TIMEOUT, (CONNECT_TIMEOUT/1000) + " s");
		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, unreachableEndpoint.getHostName());
		config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, unreachableEndpoint.getPort());


		try {
			JobVertex vertex = new JobVertex("Test Vertex");
			vertex.setInvokableClass(TestInvokable.class);

			final AtomicReference<Throwable> error = new AtomicReference<Throwable>();

			Thread invoker = new Thread("test invoker") {
				@Override
				public void run() {
					try {
						new Client(config);
						fail("This should fail with an exception since the JobManager is unreachable.");
					}
					catch (Throwable t) {
						synchronized (error) {
							error.set(t);
							error.notifyAll();
						}
					}
				}
			};

			invoker.setDaemon(true);
			invoker.start();

			try {
				// wait until the caller is successful, for at most the given time
				long now = System.currentTimeMillis();
				long deadline = now + MAX_DELAY;

				synchronized (error) {
					while (invoker.isAlive() && error.get() == null && now < deadline) {
						error.wait(1000);
						now = System.currentTimeMillis();
					}
				}

				Throwable t = error.get();
				if (t == null) {
					fail("Job invocation did not fail in expected time interval.");
				}
				else {
					assertNotNull(t.getMessage());
					assertTrue(t.getMessage(), t.getMessage().contains("JobManager"));
				}
			}
			finally {
				if (invoker.isAlive()) {
					invoker.interrupt();
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// --------------------------------------------------------------------------------------------

	public static class TestInvokable extends AbstractInvokable {
		@Override
		public void registerInputOutput() {}

		@Override
		public void invoke() {}
	}
}
