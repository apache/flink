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
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * Tests for the utility methods in {@link FutureUtils}
 */
public class FutureUtilsTest {

	@Test
	public void testConjunctFutureFailsOnEmptyAndNull() throws Exception {
		try {
			FutureUtils.combineAll(null);
			fail();
		} catch (NullPointerException ignored) {}

		try {
			FutureUtils.combineAll(Arrays.asList(
					new FlinkCompletableFuture<Object>(),
					null,
					new FlinkCompletableFuture<Object>()));
			fail();
		} catch (NullPointerException ignored) {}
	}

	@Test
	public void testConjunctFutureCompletion() throws Exception {
		// some futures that we combine
		CompletableFuture<Object> future1 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future2 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future3 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future4 = new FlinkCompletableFuture<>();

		// some future is initially completed
		future2.complete(new Object());

		// build the conjunct future
		ConjunctFuture result = FutureUtils.combineAll(Arrays.asList(future1, future2, future3, future4));

		Future<Void> resultMapped = result.thenAccept(new AcceptFunction<Void>() {
			@Override
			public void accept(Void value) {}
		});

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

		CompletableFuture<Object> future1 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future2 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future3 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future4 = new FlinkCompletableFuture<>();

		// build the conjunct future
		ConjunctFuture result = FutureUtils.combineAll(Arrays.asList(future1, future2, future3, future4));

		Future<Void> resultMapped = result.thenAccept(new AcceptFunction<Void>() {
			@Override
			public void accept(Void value) {}
		});

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

		CompletableFuture<Object> future1 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future2 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future3 = new FlinkCompletableFuture<>();
		CompletableFuture<Object> future4 = new FlinkCompletableFuture<>();

		// build the conjunct future
		ConjunctFuture result = FutureUtils.combineAll(Arrays.asList(future1, future2, future3, future4));
		assertEquals(4, result.getNumFuturesTotal());

		Future<Void> resultMapped = result.thenAccept(new AcceptFunction<Void>() {
			@Override
			public void accept(Void value) {}
		});

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

	@Test
	public void testConjunctOfNone() throws Exception {
		final ConjunctFuture result = FutureUtils.combineAll(Collections.<Future<Object>>emptyList());

		assertEquals(0, result.getNumFuturesTotal());
		assertEquals(0, result.getNumFuturesCompleted());
		assertTrue(result.isDone());
	}
}
