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

package org.apache.flink.runtime.highavailability.nonha.leaderelection;

import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.StringUtils;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * Tests for the {@link SingleLeaderElectionService}.
 */
public class SingleLeaderElectionServiceTest {

	private static final Random RND = new Random();

	private final Executor executor = Executors.directExecutor();

	// ------------------------------------------------------------------------

	@Test
	public void testStartStopAssignLeadership() throws Exception {
		final UUID uuid = UUID.randomUUID();
		final SingleLeaderElectionService service = new SingleLeaderElectionService(executor, uuid);

		final LeaderContender contender = mockContender(service);
		final LeaderContender otherContender = mockContender(service);

		service.start(contender);
		verify(contender, times(1)).grantLeadership(uuid);

		service.stop();
		verify(contender, times(1)).revokeLeadership();

		// start with a new contender - the old contender must not gain another leadership
		service.start(otherContender);
		verify(otherContender, times(1)).grantLeadership(uuid);

		verify(contender, times(1)).grantLeadership(uuid);
		verify(contender, times(1)).revokeLeadership();
	}

	@Test
	public void testStopBeforeConfirmingLeadership() throws Exception {
		final UUID uuid = UUID.randomUUID();
		final SingleLeaderElectionService service = new SingleLeaderElectionService(executor, uuid);

		final LeaderContender contender = mock(LeaderContender.class);

		service.start(contender);
		verify(contender, times(1)).grantLeadership(uuid);

		service.stop();

		// because the leadership was never confirmed, there is no "revoke" call
		verifyNoMoreInteractions(contender);
	}

	@Test
	public void testStartOnlyOnce() throws Exception {
		final UUID uuid = UUID.randomUUID();
		final SingleLeaderElectionService service = new SingleLeaderElectionService(executor, uuid);

		final LeaderContender contender = mock(LeaderContender.class);
		final LeaderContender otherContender = mock(LeaderContender.class);

		service.start(contender);
		verify(contender, times(1)).grantLeadership(uuid);

		// should not be possible to start again this with another contender
		try {
			service.start(otherContender);
			fail("should fail with an exception");
		} catch (IllegalStateException e) {
			// expected
		}

		// should not be possible to start this again with the same contender
		try {
			service.start(contender);
			fail("should fail with an exception");
		} catch (IllegalStateException e) {
			// expected
		}
	}

	@Test
	public void testShutdown() throws Exception {
		final UUID uuid = UUID.randomUUID();
		final SingleLeaderElectionService service = new SingleLeaderElectionService(executor, uuid);

		// create a leader contender and let it grab leadership
		final LeaderContender contender = mockContender(service);
		service.start(contender);
		verify(contender, times(1)).grantLeadership(uuid);

		// some leader listeners
		final LeaderRetrievalListener listener1 = mock(LeaderRetrievalListener.class);
		final LeaderRetrievalListener listener2 = mock(LeaderRetrievalListener.class);

		LeaderRetrievalService listenerService1 = service.createLeaderRetrievalService();
		LeaderRetrievalService listenerService2 = service.createLeaderRetrievalService();

		listenerService1.start(listener1);
		listenerService2.start(listener2);

		// one listener stops
		listenerService1.stop();

		// shut down the service
		service.shutdown();

		// the leader contender and running listener should get error notifications
		verify(contender, times(1)).handleError(any(Exception.class));
		verify(listener2, times(1)).handleError(any(Exception.class));

		// the stopped listener gets no notification
		verify(listener1, times(0)).handleError(any(Exception.class));

		// should not be possible to start again after shutdown
		try {
			service.start(contender);
			fail("should fail with an exception");
		} catch (IllegalStateException e) {
			// expected
		}

		// no additional leadership grant
		verify(contender, times(1)).grantLeadership(any(UUID.class));
	}

	@Test
	public void testImmediateShutdown() throws Exception {
		final UUID uuid = UUID.randomUUID();
		final SingleLeaderElectionService service = new SingleLeaderElectionService(executor, uuid);
		service.shutdown();

		final LeaderContender contender = mock(LeaderContender.class);
		
		// should not be possible to start
		try {
			service.start(contender);
			fail("should fail with an exception");
		} catch (IllegalStateException e) {
			// expected
		}

		// no additional leadership grant
		verify(contender, times(0)).grantLeadership(any(UUID.class));
	}

//	@Test
//	public void testNotifyListenersWhenLeaderElected() throws Exception {
//		final UUID uuid = UUID.randomUUID();
//		final SingleLeaderElectionService service = new SingleLeaderElectionService(executor, uuid);
//
//		final LeaderRetrievalListener listener1 = mock(LeaderRetrievalListener.class);
//		final LeaderRetrievalListener listener2 = mock(LeaderRetrievalListener.class);
//
//		LeaderRetrievalService listenerService1 = service.createLeaderRetrievalService();
//		LeaderRetrievalService listenerService2 = service.createLeaderRetrievalService();
//
//		listenerService1.start(listener1);
//		listenerService1.start(listener2);
//
//		final LeaderContender contender = mockContender(service);
//		service.start(contender);
//
//		veri
//	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private static LeaderContender mockContender(final LeaderElectionService service) {
		String address = StringUtils.getRandomString(RND, 5, 10, 'a', 'z');
		return mockContender(service, address);
	}

	private static LeaderContender mockContender(final LeaderElectionService service, final String address) {
		LeaderContender mockContender = mock(LeaderContender.class);

		when(mockContender.getAddress()).thenReturn(address);

		doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					final UUID uuid = (UUID) invocation.getArguments()[0];
					service.confirmLeaderSessionID(uuid);
					return null;
				}
		}).when(mockContender).grantLeadership(any(UUID.class));

		return mockContender;
	}
}
