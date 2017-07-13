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

package org.apache.flink.runtime.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.query.AkkaKvStateLocationLookupService.LookupRetryStrategy;
import org.apache.flink.runtime.query.AkkaKvStateLocationLookupService.LookupRetryStrategyFactory;
import org.apache.flink.runtime.query.KvStateMessage.LookupKvStateLocation;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link AkkaKvStateLocationLookupService}.
 */
public class AkkaKvStateLocationLookupServiceTest extends TestLogger {

	/** The default timeout. */
	private static final FiniteDuration TIMEOUT = new FiniteDuration(10, TimeUnit.SECONDS);

	/** Test actor system shared between the tests. */
	private static ActorSystem testActorSystem;

	@BeforeClass
	public static void setUp() throws Exception {
		testActorSystem = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (testActorSystem != null) {
			testActorSystem.shutdown();
		}
	}

	/**
	 * Tests responses if no leader notification has been reported or leadership
	 * has been lost (leaderAddress = <code>null</code>).
	 */
	@Test
	public void testNoJobManagerRegistered() throws Exception {
		TestingLeaderRetrievalService leaderRetrievalService = new TestingLeaderRetrievalService(
			null,
			null);
		Queue<LookupKvStateLocation> received = new LinkedBlockingQueue<>();

		AkkaKvStateLocationLookupService lookupService = new AkkaKvStateLocationLookupService(
				leaderRetrievalService,
				testActorSystem,
				TIMEOUT,
				new AkkaKvStateLocationLookupService.DisabledLookupRetryStrategyFactory());

		lookupService.start();

		//
		// No leader registered initially => fail with UnknownJobManager
		//
		try {
			JobID jobId = new JobID();
			String name = "coffee";

			Future<KvStateLocation> locationFuture = lookupService.getKvStateLookupInfo(jobId, name);

			Await.result(locationFuture, TIMEOUT);
			fail("Did not throw expected Exception");
		} catch (UnknownJobManager ignored) {
			// Expected
		}

		assertEquals("Received unexpected lookup", 0, received.size());

		//
		// Leader registration => communicate with new leader
		//
		UUID leaderSessionId = HighAvailabilityServices.DEFAULT_LEADER_ID;
		KvStateLocation expected = new KvStateLocation(new JobID(), new JobVertexID(), 8282, "tea");

		ActorRef testActor = LookupResponseActor.create(received, leaderSessionId, expected);

		String testActorAddress = AkkaUtils.getAkkaURL(testActorSystem, testActor);

		// Notify the service about a leader
		leaderRetrievalService.notifyListener(testActorAddress, leaderSessionId);

		JobID jobId = new JobID();
		String name = "tea";

		// Verify that the leader response is handled
		KvStateLocation location = Await.result(lookupService.getKvStateLookupInfo(jobId, name), TIMEOUT);
		assertEquals(expected, location);

		// Verify that the correct message was sent to the leader
		assertEquals(1, received.size());

		verifyLookupMsg(received.poll(), jobId, name);

		//
		// Leader loss => fail with UnknownJobManager
		//
		leaderRetrievalService.notifyListener(null, null);

		try {
			Future<KvStateLocation> locationFuture = lookupService
					.getKvStateLookupInfo(new JobID(), "coffee");

			Await.result(locationFuture, TIMEOUT);
			fail("Did not throw expected Exception");
		} catch (UnknownJobManager ignored) {
			// Expected
		}

		// No new messages received
		assertEquals(0, received.size());
	}

	/**
	 * Tests that messages are properly decorated with the leader session ID.
	 */
	@Test
	public void testLeaderSessionIdChange() throws Exception {
		TestingLeaderRetrievalService leaderRetrievalService = new TestingLeaderRetrievalService(
			"localhost",
			HighAvailabilityServices.DEFAULT_LEADER_ID);
		Queue<LookupKvStateLocation> received = new LinkedBlockingQueue<>();

		AkkaKvStateLocationLookupService lookupService = new AkkaKvStateLocationLookupService(
				leaderRetrievalService,
				testActorSystem,
				TIMEOUT,
				new AkkaKvStateLocationLookupService.DisabledLookupRetryStrategyFactory());

		lookupService.start();

		// Create test actors with random leader session IDs
		KvStateLocation expected1 = new KvStateLocation(new JobID(), new JobVertexID(), 8282, "salt");
		UUID leaderSessionId1 = UUID.randomUUID();
		ActorRef testActor1 = LookupResponseActor.create(received, leaderSessionId1, expected1);
		String testActorAddress1 = AkkaUtils.getAkkaURL(testActorSystem, testActor1);

		KvStateLocation expected2 = new KvStateLocation(new JobID(), new JobVertexID(), 22321, "pepper");
		UUID leaderSessionId2 = UUID.randomUUID();
		ActorRef testActor2 = LookupResponseActor.create(received, leaderSessionId1, expected2);
		String testActorAddress2 = AkkaUtils.getAkkaURL(testActorSystem, testActor2);

		JobID jobId = new JobID();

		//
		// Notify about first leader
		//
		leaderRetrievalService.notifyListener(testActorAddress1, leaderSessionId1);

		KvStateLocation location = Await.result(lookupService.getKvStateLookupInfo(jobId, "rock"), TIMEOUT);
		assertEquals(expected1, location);

		assertEquals(1, received.size());
		verifyLookupMsg(received.poll(), jobId, "rock");

		//
		// Notify about second leader
		//
		leaderRetrievalService.notifyListener(testActorAddress2, leaderSessionId2);

		location = Await.result(lookupService.getKvStateLookupInfo(jobId, "roll"), TIMEOUT);
		assertEquals(expected2, location);

		assertEquals(1, received.size());
		verifyLookupMsg(received.poll(), jobId, "roll");
	}

	/**
	 * Tests that lookups are retried when no leader notification is available.
	 */
	@Test
	public void testRetryOnUnknownJobManager() throws Exception {
		final Queue<LookupRetryStrategy> retryStrategies = new LinkedBlockingQueue<>();

		LookupRetryStrategyFactory retryStrategy =
				new LookupRetryStrategyFactory() {
					@Override
					public LookupRetryStrategy createRetryStrategy() {
						return retryStrategies.poll();
					}
				};

		final TestingLeaderRetrievalService leaderRetrievalService = new TestingLeaderRetrievalService(
			null,
			null);

		AkkaKvStateLocationLookupService lookupService = new AkkaKvStateLocationLookupService(
				leaderRetrievalService,
				testActorSystem,
				TIMEOUT,
				retryStrategy);

		lookupService.start();

		//
		// Test call to retry
		//
		final AtomicBoolean hasRetried = new AtomicBoolean();
		retryStrategies.add(
				new LookupRetryStrategy() {
					@Override
					public FiniteDuration getRetryDelay() {
						return FiniteDuration.Zero();
					}

					@Override
					public boolean tryRetry() {
						if (hasRetried.compareAndSet(false, true)) {
							return true;
						}
						return false;
					}
				});

		Future<KvStateLocation> locationFuture = lookupService.getKvStateLookupInfo(new JobID(), "yessir");

		Await.ready(locationFuture, TIMEOUT);
		assertTrue("Did not retry ", hasRetried.get());

		//
		// Test leader notification after retry
		//
		Queue<LookupKvStateLocation> received = new LinkedBlockingQueue<>();

		KvStateLocation expected = new KvStateLocation(new JobID(), new JobVertexID(), 12122, "garlic");
		ActorRef testActor = LookupResponseActor.create(received, null, expected);
		final String testActorAddress = AkkaUtils.getAkkaURL(testActorSystem, testActor);

		retryStrategies.add(new LookupRetryStrategy() {
			@Override
			public FiniteDuration getRetryDelay() {
				return FiniteDuration.apply(100, TimeUnit.MILLISECONDS);
			}

			@Override
			public boolean tryRetry() {
				leaderRetrievalService.notifyListener(testActorAddress, HighAvailabilityServices.DEFAULT_LEADER_ID);
				return true;
			}
		});

		KvStateLocation location = Await.result(lookupService.getKvStateLookupInfo(new JobID(), "yessir"), TIMEOUT);
		assertEquals(expected, location);
	}

	@Test
	public void testUnexpectedResponseType() throws Exception {
		TestingLeaderRetrievalService leaderRetrievalService = new TestingLeaderRetrievalService(
			"localhost",
			HighAvailabilityServices.DEFAULT_LEADER_ID);
		Queue<LookupKvStateLocation> received = new LinkedBlockingQueue<>();

		AkkaKvStateLocationLookupService lookupService = new AkkaKvStateLocationLookupService(
				leaderRetrievalService,
				testActorSystem,
				TIMEOUT,
				new AkkaKvStateLocationLookupService.DisabledLookupRetryStrategyFactory());

		lookupService.start();

		// Create test actors with random leader session IDs
		String expected = "unexpected-response-type";
		ActorRef testActor = LookupResponseActor.create(received, null, expected);
		String testActorAddress = AkkaUtils.getAkkaURL(testActorSystem, testActor);

		leaderRetrievalService.notifyListener(testActorAddress, null);

		try {
			Await.result(lookupService.getKvStateLookupInfo(new JobID(), "spicy"), TIMEOUT);
			fail("Did not throw expected Exception");
		} catch (Throwable ignored) {
			// Expected
		}
	}

	private static final class LookupResponseActor extends FlinkUntypedActor {

		/** Received lookup messages. */
		private final Queue<LookupKvStateLocation> receivedLookups;

		/** Responses on KvStateMessage.LookupKvStateLocation messages. */
		private final Queue<Object> lookupResponses;

		/** The leader session ID. */
		private UUID leaderSessionId;

		public LookupResponseActor(
				Queue<LookupKvStateLocation> receivedLookups,
				UUID leaderSessionId, Object... lookupResponses) {

			this.receivedLookups = Preconditions.checkNotNull(receivedLookups, "Received lookups");
			this.leaderSessionId = leaderSessionId;
			this.lookupResponses = new ArrayDeque<>();

			if (lookupResponses != null) {
				for (Object resp : lookupResponses) {
					this.lookupResponses.add(resp);
				}
			}
		}

		@Override
		public void handleMessage(Object message) throws Exception {
			if (message instanceof LookupKvStateLocation) {
				// Add to received lookups queue
				receivedLookups.add((LookupKvStateLocation) message);

				Object msg = lookupResponses.poll();
				if (msg != null) {
					if (msg instanceof Throwable) {
						sender().tell(new Status.Failure((Throwable) msg), self());
					} else {
						sender().tell(new Status.Success(msg), self());
					}
				}
			} else if (message instanceof UUID) {
				this.leaderSessionId = (UUID) message;
			} else {
				LOG.debug("Received unhandled message: {}", message);
			}
		}

		@Override
		protected UUID getLeaderSessionID() {
			return leaderSessionId;
		}

		private static ActorRef create(
				Queue<LookupKvStateLocation> receivedLookups,
				UUID leaderSessionId,
				Object... lookupResponses) {

			return testActorSystem.actorOf(Props.create(
					LookupResponseActor.class,
					receivedLookups,
					leaderSessionId,
					lookupResponses));
		}
	}

	private static void verifyLookupMsg(
			LookupKvStateLocation lookUpMsg,
			JobID expectedJobId,
			String expectedName) {

		assertNotNull(lookUpMsg);
		assertEquals(expectedJobId, lookUpMsg.getJobId());
		assertEquals(expectedName, lookUpMsg.getRegistrationName());
	}

}
