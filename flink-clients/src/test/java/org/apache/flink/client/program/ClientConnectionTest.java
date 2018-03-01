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

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobClientActorTest;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test starts a job client without the JobManager being reachable. It
 * tests for a timely error and a meaningful error message.
 */
public class ClientConnectionTest extends TestLogger {

	private static final long CONNECT_TIMEOUT = 100L; // 100 ms
	private static final long ASK_STARTUP_TIMEOUT = 20000L; // 10 seconds

	/**
	 * Tests the behavior against a LOCAL address where no job manager is running.
	 */
	@Test
	public void testExceptionWhenLocalJobManagerUnreachablelocal() throws Exception {

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
	public void testExceptionWhenRemoteJobManagerUnreachable() throws Exception {

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

	private static void testFailureBehavior(final InetSocketAddress unreachableEndpoint) throws Exception {

		final Configuration config = new Configuration();
		config.setString(AkkaOptions.ASK_TIMEOUT, ASK_STARTUP_TIMEOUT + " ms");
		config.setString(AkkaOptions.LOOKUP_TIMEOUT, CONNECT_TIMEOUT + " ms");
		config.setString(JobManagerOptions.ADDRESS, unreachableEndpoint.getHostName());
		config.setInteger(JobManagerOptions.PORT, unreachableEndpoint.getPort());

		StandaloneClusterClient client = new StandaloneClusterClient(config);

		try {
			// we have to query the cluster status to start the connection attempts
			client.getClusterStatus();
			fail("This should fail with an exception since the endpoint is unreachable.");
		} catch (Exception e) {
			// check that we have failed with a LeaderRetrievalException which says that we could
			// not connect to the leading JobManager
			assertTrue(CommonTestUtils.containsCause(e, LeaderRetrievalException.class));
		}
	}

	/**
	 * FLINK-6629
	 *
	 * <p>Tests that the {@link HighAvailabilityServices} are respected when initializing the ClusterClient's
	 * {@link ActorSystem} and retrieving the leading JobManager.
	 */
	@Test
	public void testJobManagerRetrievalWithHAServices() throws Exception {
		final Configuration configuration = new Configuration();
		final TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		final ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();
		ActorRef actorRef = null;
		final UUID leaderId = UUID.randomUUID();

		try {
			actorRef = actorSystem.actorOf(
				Props.create(
					JobClientActorTest.PlainActor.class,
					leaderId));

			final String expectedAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);

			final SettableLeaderRetrievalService settableLeaderRetrievalService = new SettableLeaderRetrievalService(expectedAddress, leaderId);

			highAvailabilityServices.setJobMasterLeaderRetriever(HighAvailabilityServices.DEFAULT_JOB_ID, settableLeaderRetrievalService);

			StandaloneClusterClient client = new StandaloneClusterClient(configuration, highAvailabilityServices, true);

			ActorGateway gateway = client.getJobManagerGateway();

			assertEquals(expectedAddress, gateway.path());
			assertEquals(leaderId, gateway.leaderSessionID());
		} finally {
			if (actorRef != null) {
				TestingUtils.stopActorGracefully(actorRef);
			}

			actorSystem.shutdown();
		}
	}
}
