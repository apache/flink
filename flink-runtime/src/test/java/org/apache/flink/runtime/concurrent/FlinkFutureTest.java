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

import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.concurrent.impl.FlinkFuture;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for Flink's future implementation.
 */
public class FlinkFutureTest extends TestLogger {

	private static ExecutorService executor;

	@BeforeClass
	public static void setup() {
		executor = Executors.newSingleThreadExecutor();
	}

	@AfterClass
	public static void teardown() {
		executor.shutdown();
	}

	@Test(timeout = 10000L)
	public void testFutureApplyAsync() throws Exception {
		int expectedValue = 42;

		CompletableFuture<Integer> initialFuture = new FlinkCompletableFuture<>();

		Future<String> appliedFuture = initialFuture.thenApplyAsync(new ApplyFunction<Integer, String>() {
			@Override
			public String apply(Integer value) {
				return String.valueOf(value);
			}
		}, executor);

		initialFuture.complete(expectedValue);

		assertEquals(String.valueOf(expectedValue), appliedFuture.get());
	}

	@Test(expected = TimeoutException.class)
	public void testFutureGetTimeout() throws InterruptedException, ExecutionException, TimeoutException {
		CompletableFuture<Integer> future = new FlinkCompletableFuture<>();

		future.get(10, TimeUnit.MILLISECONDS);

		fail("Get should have thrown a timeout exception.");
	}

	@Test(expected = TestException.class)
	public void testExceptionalCompletion() throws Throwable {
		CompletableFuture<Integer> initialFuture = new FlinkCompletableFuture<>();

		initialFuture.completeExceptionally(new TestException("Test exception"));

		try {
			initialFuture.get();

			fail("Get should have thrown an exception.");
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	/**
	 * Tests that an exception is propagated through an apply function.
	 */
	@Test(expected = TestException.class)
	public void testExceptionPropagation() throws Throwable {
		CompletableFuture<Integer> initialFuture = new FlinkCompletableFuture<>();

		Future<String> mappedFuture = initialFuture.thenApplyAsync(new ApplyFunction<Integer, String>() {
			@Override
			public String apply(Integer value) {
				throw new TestException("Test exception");
			}
		}, executor);

		Future<String> mapped2Future = mappedFuture.thenApplyAsync(new ApplyFunction<String, String>() {
			@Override
			public String apply(String value) {
				return "foobar";
			}
		}, executor);

		initialFuture.complete(42);

		try {
			mapped2Future.get();

			fail("Get should have thrown an exception.");
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	@Test(timeout = 10000L)
	public void testExceptionallyAsync() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> initialFuture = new FlinkCompletableFuture<>();
		String exceptionMessage = "Foobar";

		Future<String> recovered = initialFuture.exceptionallyAsync(new ApplyFunction<Throwable, String>() {
			@Override
			public String apply(Throwable value) {
				return value.getMessage();
			}
		}, executor);

		initialFuture.completeExceptionally(new TestException(exceptionMessage));

		String actualMessage = recovered.get();

		assertEquals(exceptionMessage, actualMessage);
	}

	@Test(timeout = 10000L)
	public void testComposeAsync() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> initialFuture = new FlinkCompletableFuture<>();

		final int expectedValue = 42;

		Future<Integer> composedFuture = initialFuture.thenComposeAsync(new ApplyFunction<Integer, Future<Integer>>() {
			@Override
			public Future<Integer> apply(Integer value) {
				return FlinkFuture.supplyAsync(new Callable<Integer>() {
					@Override
					public Integer call() throws Exception {
						return expectedValue;
					}
				}, executor);
			}
		}, executor);

		initialFuture.complete(42);

		int actualValue = composedFuture.get();

		assertEquals(expectedValue, actualValue);
	}

	@Test(timeout = 10000L)
	public void testCombineAsync() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> leftFuture = new FlinkCompletableFuture<>();
		CompletableFuture<String> rightFuture = new FlinkCompletableFuture<>();

		final int expectedLeftValue = 42;
		final String expectedRightValue = "foobar";


		Future<String> resultFuture = leftFuture.thenCombineAsync(rightFuture, new BiFunction<Integer, String, String>() {
			@Override
			public String apply(Integer integer, String s) {
				return s + integer;
			}
		}, executor);

		leftFuture.complete(expectedLeftValue);
		rightFuture.complete(expectedRightValue);

		String result = resultFuture.get();

		assertEquals(expectedRightValue + expectedLeftValue, result);
	}

	@Test(timeout = 10000L)
	public void testCombineAsyncLeftFailure() throws InterruptedException {
		CompletableFuture<Integer> leftFuture = new FlinkCompletableFuture<>();
		CompletableFuture<String> rightFuture = new FlinkCompletableFuture<>();

		final String expectedRightValue = "foobar";
		final TestException testException = new TestException("barfoo");


		Future<String> resultFuture = leftFuture.thenCombineAsync(rightFuture, new BiFunction<Integer, String, String>() {
			@Override
			public String apply(Integer integer, String s) {
				return s + integer;
			}
		}, executor);

		leftFuture.completeExceptionally(testException);
		rightFuture.complete(expectedRightValue);

		try {
			resultFuture.get();
			fail("We should have caught an ExecutionException.");
		} catch (ExecutionException e) {
			assertEquals(testException, e.getCause());
		}
	}

	@Test(timeout = 10000L)
	public void testCombineAsyncRightFailure() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> leftFuture = new FlinkCompletableFuture<>();
		CompletableFuture<String> rightFuture = new FlinkCompletableFuture<>();

		final int expectedLeftValue = 42;
		final TestException testException = new TestException("barfoo");


		Future<String> resultFuture = leftFuture.thenCombineAsync(rightFuture, new BiFunction<Integer, String, String>() {
			@Override
			public String apply(Integer integer, String s) {
				return s + integer;
			}
		}, executor);

		leftFuture.complete(expectedLeftValue);
		rightFuture.completeExceptionally(testException);

		try {
			resultFuture.get();
			fail("We should have caught an ExecutionException.");
		} catch (ExecutionException e) {
			assertEquals(testException, e.getCause());
		}
	}

	@Test
	public void testGetNow() throws ExecutionException {
		CompletableFuture<Integer> initialFuture = new FlinkCompletableFuture<>();

		final int absentValue = 41;

		assertEquals(new Integer(absentValue), initialFuture.getNow(absentValue));
	}

	@Test(timeout = 10000L)
	public void testAcceptAsync() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> initialFuture = new FlinkCompletableFuture<>();
		final AtomicInteger atomicInteger = new AtomicInteger(0);
		int expectedValue = 42;

		Future<Void> result = initialFuture.thenAcceptAsync(new AcceptFunction<Integer>() {
			@Override
			public void accept(Integer value) {
				atomicInteger.set(value);
			}
		}, executor);

		initialFuture.complete(expectedValue);

		result.get();

		assertEquals(expectedValue, atomicInteger.get());
	}

	@Test(timeout = 10000L)
	public void testHandleAsync() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> initialFuture = new FlinkCompletableFuture<>();
		int expectedValue = 43;

		Future<String> result = initialFuture.handleAsync(new BiFunction<Integer, Throwable, String>() {
			@Override
			public String apply(Integer integer, Throwable throwable) {
				if (integer != null) {
					return String.valueOf(integer);
				} else {
					return throwable.getMessage();
				}
			}
		}, executor);

		initialFuture.complete(expectedValue);

		assertEquals(String.valueOf(expectedValue), result.get());
	}

	@Test(timeout = 10000L)
	public void testHandleAsyncException() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> initialFuture = new FlinkCompletableFuture<>();
		String exceptionMessage = "foobar";

		Future<String> result = initialFuture.handleAsync(new BiFunction<Integer, Throwable, String>() {
			@Override
			public String apply(Integer integer, Throwable throwable) {
				if (integer != null) {
					return String.valueOf(integer);
				} else {
					return throwable.getMessage();
				}
			}
		}, executor);

		initialFuture.completeExceptionally(new TestException(exceptionMessage));

		assertEquals(exceptionMessage, result.get());
	}

	@Test(timeout = 10000L)
	public void testMultipleCompleteOperations() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> initialFuture = new FlinkCompletableFuture<>();
		int expectedValue = 42;

		assertTrue(initialFuture.complete(expectedValue));

		assertFalse(initialFuture.complete(1337));

		assertFalse(initialFuture.completeExceptionally(new TestException("foobar")));

		assertEquals(new Integer(expectedValue), initialFuture.get());
	}

	@Test
	public void testApply() throws ExecutionException, InterruptedException {
		int expectedValue = 42;

		CompletableFuture<Integer> initialFuture = new FlinkCompletableFuture<>();

		Future<String> appliedFuture = initialFuture.thenApply(new ApplyFunction<Integer, String>() {
			@Override
			public String apply(Integer value) {
				return String.valueOf(value);
			}
		});

		initialFuture.complete(expectedValue);

		assertEquals(String.valueOf(expectedValue), appliedFuture.get());
	}

	@Test
	public void testAccept() throws ExecutionException, InterruptedException {
		int expectedValue = 42;
		Future<Integer> initialFuture = FlinkCompletableFuture.completed(expectedValue);
		final AtomicInteger atomicInteger = new AtomicInteger(0);

		Future<Void> result = initialFuture.thenAccept(new AcceptFunction<Integer>() {
			@Override
			public void accept(Integer value) {
				atomicInteger.set(value);
			}
		});

		result.get();

		assertEquals(expectedValue, atomicInteger.get());
	}

	@Test
	public void testExceptionally() throws ExecutionException, InterruptedException {
		String exceptionMessage = "Foobar";
		Future<Integer> initialFuture = FlinkCompletableFuture
			.completedExceptionally(new TestException(exceptionMessage));


		Future<String> recovered = initialFuture.exceptionally(new ApplyFunction<Throwable, String>() {
			@Override
			public String apply(Throwable value) {
				return value.getMessage();
			}
		});

		String actualMessage = recovered.get();

		assertEquals(exceptionMessage, actualMessage);
	}

	@Test
	public void testHandle() throws ExecutionException, InterruptedException {
		int expectedValue = 43;
		Future<Integer> initialFuture = FlinkCompletableFuture.completed(expectedValue);

		Future<String> result = initialFuture.handle(new BiFunction<Integer, Throwable, String>() {
			@Override
			public String apply(Integer integer, Throwable throwable) {
				if (integer != null) {
					return String.valueOf(integer);
				} else {
					return throwable.getMessage();
				}
			}
		});

		assertEquals(String.valueOf(expectedValue), result.get());
	}

	@Test
	public void testCompose() throws ExecutionException, InterruptedException {
		CompletableFuture<Integer> initialFuture = new FlinkCompletableFuture<>();
		final int expectedValue = 42;

		Future<Integer> composedFuture = initialFuture.thenCompose(new ApplyFunction<Integer, Future<Integer>>() {
			@Override
			public Future<Integer> apply(Integer value) {
				return FlinkFuture.supplyAsync(new Callable<Integer>() {
					@Override
					public Integer call() throws Exception {
						return expectedValue;
					}
				}, executor);
			}
		});

		initialFuture.complete(42);

		int actualValue = composedFuture.get();

		assertEquals(expectedValue, actualValue);
	}

	@Test
	public void testCombine() throws ExecutionException, InterruptedException {
		int expectedLeftValue = 1;
		int expectedRightValue = 2;

		Future<Integer> left = FlinkCompletableFuture.completed(expectedLeftValue);
		Future<Integer> right = FlinkCompletableFuture.completed(expectedRightValue);

		Future<Integer> sum = left.thenCombine(right, new BiFunction<Integer, Integer, Integer>() {
			@Override
			public Integer apply(Integer left, Integer right) {
				return left + right;
			}
		});

		int result = sum.get();

		assertEquals(expectedLeftValue + expectedRightValue, result);
	}

	/**
	 * Tests that multiple functions can be called on complete futures.
	 */
	@Test(timeout = 10000L)
	public void testMultipleFunctionsOnCompleteFuture() throws Exception {
		final FlinkCompletableFuture<String> future = FlinkCompletableFuture.completed("test");

		Future<String> result1 = future.handleAsync(new BiFunction<String, Throwable, String>() {

			@Override
			public String apply(String s, Throwable throwable) {
				return s != null ? s : throwable.getMessage();
			}
		}, executor);

		Future<Void> result2 = future.thenAcceptAsync(new AcceptFunction<String>() {
			@Override
			public void accept(String value) {}
		}, executor);

		assertEquals("test", result1.get());
		assertNull(result2.get());
	}

	/**
	 * Tests that multiple functions can be called on incomplete futures.
	 */
	@Test(timeout = 10000L)
	public void testMultipleFunctionsOnIncompleteFuture() throws Exception {
		final FlinkCompletableFuture<String> future = new FlinkCompletableFuture<>();

		Future<String> result1 = future.handleAsync(new BiFunction<String, Throwable, String>() {
			@Override
			public String apply(String s, Throwable throwable) {
				return s != null ? s : throwable.getMessage();
			}
		}, executor);

		Future<Void> result2 = future.thenAcceptAsync(new AcceptFunction<String>() {
			@Override
			public void accept(String value) {}
		}, executor);

		future.complete("value");

		assertEquals("value", result1.get());
		assertNull(result2.get());
	}

	/**
	 * Tests that multiple functions can be called on complete futures.
	 */
	@Test(timeout = 10000)
	public void testMultipleFunctionsExceptional() throws Exception {
		final FlinkCompletableFuture<String> future = new FlinkCompletableFuture<>();

		Future<String> result1 = future.handleAsync(new BiFunction<String, Throwable, String>() {
			@Override
			public String apply(String s, Throwable throwable) {
				return s != null ? s : throwable.getMessage();
			}
		}, executor);

		Future<Void> result2 = future.thenAcceptAsync(new AcceptFunction<String>() {
			@Override
			public void accept(String value) {}
		}, executor);

		future.completeExceptionally(new TestException("test"));

		assertEquals("test", result1.get());

		try {
			result2.get();
			fail("We should have caught an ExecutionException.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof TestException);
		}
	}

	/**
	 * Tests that a chain of dependent futures will be completed exceptionally if the initial future
	 * is completed exceptionally.
	 */
	@Test(timeout = 10000)
	public void testChainedFutureExceptionalCompletion() throws ExecutionException, InterruptedException {
		final FlinkCompletableFuture<String> future = new FlinkCompletableFuture<>();

		Future<String> apply = future.thenApplyAsync(new ApplyFunction<String, String>() {
			@Override
			public String apply(String value) {
				return value;
			}
		}, executor);

		Future<Throwable> applyException = apply.exceptionallyAsync(new ApplyFunction<Throwable, Throwable>() {
			@Override
			public Throwable apply(Throwable value) {
				return value;
			}
		}, executor);

		Future<Void> accept1 = future.thenAcceptAsync(new AcceptFunction<String>() {
			@Override
			public void accept(String value) {
				// noop
			}
		}, executor);

		Future<Throwable> accept1Exception = accept1.exceptionallyAsync(new ApplyFunction<Throwable, Throwable>() {
			@Override
			public Throwable apply(Throwable value) {
				return value;
			}
		}, executor);

		Future<Void> accept2 = future.thenAcceptAsync(new AcceptFunction<String>() {
			@Override
			public void accept(String value) {
				// noop
			}
		}, executor);

		Future<Throwable> accept2Exception = accept2.exceptionallyAsync(new ApplyFunction<Throwable, Throwable>() {
			@Override
			public Throwable apply(Throwable value) {
				return value;
			}
		}, executor);

		TestException testException = new TestException("test");

		// fail the initial future
		future.completeExceptionally(testException);

		assertEquals(testException, applyException.get());
		assertEquals(testException, accept1Exception.get());
		assertEquals(testException, accept2Exception.get());
	}

	private static class TestException extends RuntimeException {

		private static final long serialVersionUID = -1274022962838535130L;

		public TestException(String message) {
			super(message);
		}
	}
}
