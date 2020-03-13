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

package org.apache.flink.core.testutils;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Some reusable hamcrest matchers for Flink.
 */
public class FlinkMatchers {

	// ------------------------------------------------------------------------
	//  factories
	// ------------------------------------------------------------------------

	/**
	 * Checks whether {@link CompletableFuture} completed already exceptionally with a specific exception type.
	 */
	public static <T, E extends Throwable> FutureFailedMatcher<T> futureFailedWith(Class<E> exceptionType) {
		Objects.requireNonNull(exceptionType, "exceptionType should not be null");
		return new FutureFailedMatcher<>(exceptionType);
	}

	/**
	 * Checks whether {@link CompletableFuture} will completed exceptionally within a certain time.
	 */
	public static <T, E extends Throwable> FutureWillFailMatcher<T> futureWillCompleteExceptionally(
			Class<E> exceptionType,
			Duration timeout) {
		Objects.requireNonNull(exceptionType, "exceptionType should not be null");
		Objects.requireNonNull(timeout, "timeout should not be null");
		return new FutureWillFailMatcher<>(exceptionType, timeout);
	}

	/**
	 * Checks whether {@link CompletableFuture} will completed exceptionally within a certain time.
	 */
	public static <T> FutureWillFailMatcher<T> futureWillCompleteExceptionally(Duration timeout) {
		return futureWillCompleteExceptionally(Throwable.class, timeout);
	}

	// ------------------------------------------------------------------------

	/** This class should not be instantiated. */
	private FlinkMatchers() {}

	// ------------------------------------------------------------------------
	//  matcher implementations
	// ------------------------------------------------------------------------

	private static final class FutureFailedMatcher<T> extends TypeSafeDiagnosingMatcher<CompletableFuture<T>> {

		private final Class<? extends Throwable> expectedException;

		FutureFailedMatcher(Class<? extends Throwable> expectedException) {
			super(CompletableFuture.class);
			this.expectedException = expectedException;
		}

		@Override
		protected boolean matchesSafely(CompletableFuture<T> future, Description mismatchDescription) {
			if (!future.isDone()) {
				mismatchDescription.appendText("Future is not completed.");
				return false;
			}

			if (!future.isCompletedExceptionally()) {
				Object result = future.getNow(null);
				assert result != null;
				mismatchDescription.appendText("Future did not complete exceptionally, but instead regularly with: " + result);
				return false;
			}

			try {
				future.getNow(null);
				throw new Error();
			}
			catch (CompletionException e) {
				if (e.getCause() != null && expectedException.isAssignableFrom(e.getCause().getClass())) {
					return true;
				}

				mismatchDescription.appendText("Future completed with different exception: " + e.getCause());
				return false;
			}
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("A CompletableFuture that failed with: " + expectedException.getName());
		}
	}

	private static final class FutureWillFailMatcher<T> extends TypeSafeDiagnosingMatcher<CompletableFuture<T>> {

		private final Class<? extends Throwable> expectedException;

		private final Duration timeout;

		FutureWillFailMatcher(Class<? extends Throwable> expectedException, Duration timeout) {
			super(CompletableFuture.class);
			this.expectedException = expectedException;
			this.timeout = timeout;
		}

		@Override
		protected boolean matchesSafely(CompletableFuture<T> future, Description mismatchDescription) {
			try {
				final Object result = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
				mismatchDescription.appendText("Future did not complete exceptionally, but instead regularly with: " + result);
				return false;
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new Error("interrupted test");
			}
			catch (TimeoutException e) {
				mismatchDescription.appendText("Future did not complete withing " + timeout.toMillis() + " milliseconds.");
				return false;
			}
			catch (ExecutionException e) {
				if (e.getCause() == null || !expectedException.isAssignableFrom(e.getCause().getClass())) {
					mismatchDescription.appendText("Future completed with different exception: " + e.getCause());
					return false;
				}
				return true;
			}
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("A CompletableFuture that will failed within " +
				timeout.toMillis() + " milliseconds with: " + expectedException.getName());
		}
	}
}
