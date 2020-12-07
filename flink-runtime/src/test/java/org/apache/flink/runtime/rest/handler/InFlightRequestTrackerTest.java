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

package org.apache.flink.runtime.rest.handler;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link InFlightRequestTracker}.
 */
public class InFlightRequestTrackerTest {

	private InFlightRequestTracker inFlightRequestTracker;

	@Before
	public void setUp() {
		inFlightRequestTracker = new InFlightRequestTracker();
	}

	@Test
	public void testShouldFinishAwaitAsyncImmediatelyIfNoRequests() {
		assertTrue(inFlightRequestTracker.awaitAsync().isDone());
	}

	@Test
	public void testShouldFinishAwaitAsyncIffAllRequestsDeregistered() {
		inFlightRequestTracker.registerRequest();

		final CompletableFuture<Void> awaitFuture = inFlightRequestTracker.awaitAsync();
		assertFalse(awaitFuture.isDone());

		inFlightRequestTracker.deregisterRequest();
		assertTrue(awaitFuture.isDone());
	}

	@Test
	public void testAwaitAsyncIsIdempotent() {
		final CompletableFuture<Void> awaitFuture = inFlightRequestTracker.awaitAsync();
		assertTrue(awaitFuture.isDone());

		assertSame(
			"The reference to the future must not change",
			awaitFuture,
			inFlightRequestTracker.awaitAsync());
	}

	@Test
	public void testShouldTolerateRegisterAfterAwaitAsync() {
		final CompletableFuture<Void> awaitFuture = inFlightRequestTracker.awaitAsync();
		assertTrue(awaitFuture.isDone());

		inFlightRequestTracker.registerRequest();

		assertSame(
			"The reference to the future must not change",
			awaitFuture,
			inFlightRequestTracker.awaitAsync());
	}

	@Test
	public void testShouldNotRegisterNewRequestsAfterTermination() {
		final CompletableFuture<Void> terminationFuture = inFlightRequestTracker.awaitAsync();

		assertTrue(terminationFuture.isDone());
		assertFalse(inFlightRequestTracker.registerRequest());
	}
}
