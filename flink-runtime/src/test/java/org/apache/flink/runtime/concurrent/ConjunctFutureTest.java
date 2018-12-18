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

package org.apache.flink.runtime.concurrent;

import org.apache.flink.runtime.concurrent.FutureUtils.ConjunctFuture;
import org.apache.flink.util.TestLogger;

import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link ConjunctFuture} and {@link FutureUtils.WaitingConjunctFuture}.
 */
@RunWith(Parameterized.class)
public class ConjunctFutureTest extends TestLogger {

	@Parameterized.Parameters
	public static Collection<FutureFactory> parameters (){
		return Arrays.asList(new ConjunctFutureFactory(), new WaitingFutureFactory());
	}

	@Parameterized.Parameter
	public FutureFactory futureFactory;

	@Test
	public void testConjunctFutureFailsOnEmptyAndNull() throws Exception {
		try {
			futureFactory.createFuture(null);
			fail();
		} catch (NullPointerException ignored) {}

		try {
			futureFactory.createFuture(Arrays.asList(
					new CompletableFuture<>(),
					null,
					new CompletableFuture<>()));
			fail();
		} catch (NullPointerException ignored) {}
	}

	@Test
	public void testConjunctFutureCompletion() throws Exception {
		// some futures that we combine
		java.util.concurrent.CompletableFuture<Object> future1 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future2 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future3 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future4 = new java.util.concurrent.CompletableFuture<>();

		// some future is initially completed
		future2.complete(new Object());

		// build the conjunct future
		ConjunctFuture<?> result = futureFactory.createFuture(Arrays.asList(future1, future2, future3, future4));

		CompletableFuture<?> resultMapped = result.thenAccept(value -> {});

		assertEquals(4, result.getNumFuturesTotal());
		assertEquals(1, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		// complete two more futures
		future4.complete(new Object());
		assertEquals(2, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		future1.complete(new Object());
		assertEquals(3, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		// complete one future again
		future1.complete(new Object());
		assertEquals(3, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		// complete the final future
		future3.complete(new Object());
		assertEquals(4, result.getNumFuturesCompleted());
		assertTrue(result.isDone());
		assertTrue(resultMapped.isDone());
	}

	@Test
	public void testConjunctFutureFailureOnFirst() throws Exception {

		java.util.concurrent.CompletableFuture<Object> future1 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future2 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future3 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future4 = new java.util.concurrent.CompletableFuture<>();

		// build the conjunct future
		ConjunctFuture<?> result = futureFactory.createFuture(Arrays.asList(future1, future2, future3, future4));

		CompletableFuture<?> resultMapped = result.thenAccept(value -> {});

		assertEquals(4, result.getNumFuturesTotal());
		assertEquals(0, result.getNumFuturesCompleted());
		assertFalse(result.isDone());
		assertFalse(resultMapped.isDone());

		future2.completeExceptionally(new IOException());

		assertEquals(0, result.getNumFuturesCompleted());
		assertTrue(result.isDone());
		assertTrue(resultMapped.isDone());

		try {
			result.get();
			fail();
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IOException);
		}

		try {
			resultMapped.get();
			fail();
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IOException);
		}
	}

	@Test
	public void testConjunctFutureFailureOnSuccessive() throws Exception {

		java.util.concurrent.CompletableFuture<Object> future1 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future2 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future3 = new java.util.concurrent.CompletableFuture<>();
		java.util.concurrent.CompletableFuture<Object> future4 = new java.util.concurrent.CompletableFuture<>();

		// build the conjunct future
		ConjunctFuture<?> result = futureFactory.createFuture(Arrays.asList(future1, future2, future3, future4));
		assertEquals(4, result.getNumFuturesTotal());

		java.util.concurrent.CompletableFuture<?> resultMapped = result.thenAccept(value -> {});

		future1.complete(new Object());
		future3.complete(new Object());
		future4.complete(new Object());

		future2.completeExceptionally(new IOException());

		assertEquals(3, result.getNumFuturesCompleted());
		assertTrue(result.isDone());
		assertTrue(resultMapped.isDone());

		try {
			result.get();
			fail();
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IOException);
		}

		try {
			resultMapped.get();
			fail();
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof IOException);
		}
	}

	/**
	 * Tests that the conjunct future returns upon completion the collection of all future values.
	 */
	@Test
	public void testConjunctFutureValue() throws ExecutionException, InterruptedException {
		java.util.concurrent.CompletableFuture<Integer> future1 = java.util.concurrent.CompletableFuture.completedFuture(1);
		java.util.concurrent.CompletableFuture<Long> future2 = java.util.concurrent.CompletableFuture.completedFuture(2L);
		java.util.concurrent.CompletableFuture<Double> future3 = new java.util.concurrent.CompletableFuture<>();

		ConjunctFuture<Collection<Number>> result = FutureUtils.combineAll(Arrays.asList(future1, future2, future3));

		assertFalse(result.isDone());

		future3.complete(.1);

		assertTrue(result.isDone());

		assertThat(result.get(), IsIterableContainingInAnyOrder.<Number>containsInAnyOrder(1, 2L, .1));
	}

	@Test
	public void testConjunctOfNone() throws Exception {
		final ConjunctFuture<?> result = futureFactory.createFuture(Collections.<java.util.concurrent.CompletableFuture<Object>>emptyList());

		assertEquals(0, result.getNumFuturesTotal());
		assertEquals(0, result.getNumFuturesCompleted());
		assertTrue(result.isDone());
	}

	/**
	 * Factory to create {@link ConjunctFuture} for testing.
	 */
	private interface FutureFactory {
		ConjunctFuture<?> createFuture(Collection<? extends java.util.concurrent.CompletableFuture<?>> futures);
	}

	private static class ConjunctFutureFactory implements FutureFactory {

		@Override
		public ConjunctFuture<?> createFuture(Collection<? extends java.util.concurrent.CompletableFuture<?>> futures) {
			return FutureUtils.combineAll(futures);
		}
	}

	private static class WaitingFutureFactory implements FutureFactory {

		@Override
		public ConjunctFuture<?> createFuture(Collection<? extends java.util.concurrent.CompletableFuture<?>> futures) {
			return FutureUtils.waitForAll(futures);
		}
	}
}
