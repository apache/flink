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
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;

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
	public static <T> FutureWillFailMatcher<T> futureWillCompleteExceptionally(
			Function<Throwable, Boolean> exceptionCheck,
			Duration timeout,
			String checkDescription) {
		Objects.requireNonNull(exceptionCheck, "exceptionType should not be null");
		Objects.requireNonNull(timeout, "timeout should not be null");
		return new FutureWillFailMatcher<>(exceptionCheck, timeout, checkDescription);
	}

	/**
	 * Checks whether {@link CompletableFuture} will completed exceptionally within a certain time.
	 */
	public static <T> FutureWillFailMatcher<T> futureWillCompleteExceptionally(Duration timeout) {
		return futureWillCompleteExceptionally(Throwable.class, timeout);
	}

	/**
	 * Checks for a {@link Throwable} that matches by class and message.
	 */
	public static Matcher<Throwable> containsCause(Throwable failureCause) {
		return new ContainsCauseMatcher(failureCause);
	}

	/**
	 * Checks for a {@link Throwable} that contains the expected error message.
	 */
	public static Matcher<Throwable> containsMessage(String errorMessage) {
		return new ContainsMessageMatcher(errorMessage);
	}

	/**
	 * Checks that a {@link CompletableFuture} won't complete within the given timeout.
	 */
	public static Matcher<CompletableFuture<?>> willNotComplete(Duration timeout) {
		return new WillNotCompleteMatcher(timeout);
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

		private final Function<Throwable, Boolean> exceptionValidator;

		private final Duration timeout;

		private final String validationDescription;

		FutureWillFailMatcher(
			Class<? extends Throwable> expectedException,
			Duration timeout) {

			super(CompletableFuture.class);
			this.exceptionValidator = (e) -> expectedException.isAssignableFrom(e.getClass());
			this.timeout = timeout;
			this.validationDescription = expectedException.getName();
		}

		FutureWillFailMatcher(
				Function<Throwable, Boolean> exceptionValidator,
				Duration timeout,
				String validationDescription) {

			super(CompletableFuture.class);
			this.exceptionValidator = exceptionValidator;
			this.timeout = timeout;
			this.validationDescription = validationDescription;
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
				final Throwable cause = e.getCause();
				if (cause != null && exceptionValidator.apply(cause)) {
					return true;
				}

				String otherDescription = "(null)";
				if (cause != null) {
					final StringWriter stm = new StringWriter();
					try (PrintWriter wrt = new PrintWriter(stm)) {
						cause.printStackTrace(wrt);
					}
					otherDescription = stm.toString();
				}

				mismatchDescription.appendText("Future completed with different exception: " + otherDescription);
				return false;
			}
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("A CompletableFuture that will have failed within " +
				timeout.toMillis() + " milliseconds with: " + validationDescription);
		}
	}

	private static final class ContainsCauseMatcher extends TypeSafeDiagnosingMatcher<Throwable> {

		private final Throwable failureCause;

		private ContainsCauseMatcher(Throwable failureCause) {
			this.failureCause = failureCause;
		}

		@Override
		protected boolean matchesSafely(Throwable throwable, Description description) {
			final Optional<Throwable> optionalCause = findThrowable(
				throwable,
				cause ->
					cause.getClass() == failureCause.getClass() &&
						cause.getMessage().equals(failureCause.getMessage()));

			if (!optionalCause.isPresent()) {
				description
					.appendText("The throwable ")
					.appendValue(throwable)
					.appendText(" does not contain the expected failure cause ")
					.appendValue(failureCause);
			}

			return optionalCause.isPresent();
		}

		@Override
		public void describeTo(Description description) {
			description
				.appendText("Expected failure cause is ")
				.appendValue(failureCause);
		}

		// copied from flink-core to not mess up the dependency design too much, just for a little
		// utility method
		private static Optional<Throwable> findThrowable(
				Throwable throwable,
				Predicate<Throwable> predicate) {
			if (throwable == null || predicate == null) {
				return Optional.empty();
			}

			Throwable t = throwable;
			while (t != null) {
				if (predicate.test(t)) {
					return Optional.of(t);
				} else {
					t = t.getCause();
				}
			}

			return Optional.empty();
		}
	}

	private static final class ContainsMessageMatcher extends TypeSafeDiagnosingMatcher<Throwable> {

		private final String errorMessage;

		private ContainsMessageMatcher(String errorMessage) {
			this.errorMessage = errorMessage;
		}

		@Override
		protected boolean matchesSafely(Throwable throwable, Description description) {
			final Optional<Throwable> optionalCause = findThrowableWithMessage(throwable, errorMessage);

			if (!optionalCause.isPresent()) {
				description
					.appendText("The throwable ")
					.appendValue(throwable)
					.appendText(" does not contain the expected error message ")
					.appendValue(errorMessage);
			}

			return optionalCause.isPresent();
		}

		@Override
		public void describeTo(Description description) {
			description
				.appendText("Expected error message is ")
				.appendValue(errorMessage);
		}

		// copied from flink-core to not mess up the dependency design too much, just for a little
		// utility method
		private static Optional<Throwable> findThrowableWithMessage(Throwable throwable, String searchMessage) {
			if (throwable == null || searchMessage == null) {
				return Optional.empty();
			}

			Throwable t = throwable;
			while (t != null) {
				if (t.getMessage() != null && t.getMessage().contains(searchMessage)) {
					return Optional.of(t);
				} else {
					t = t.getCause();
				}
			}

			return Optional.empty();
		}
	}

	private static final class WillNotCompleteMatcher extends TypeSafeDiagnosingMatcher<CompletableFuture<?>> {

		private final Duration timeout;

		private WillNotCompleteMatcher(Duration timeout) {
			this.timeout = timeout;
		}

		@Override
		protected boolean matchesSafely(
				CompletableFuture<?> item,
				Description mismatchDescription) {

			try {
				final Object value = item.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
				mismatchDescription.appendText("The given future completed with ")
						.appendValue(value);
			} catch (TimeoutException timeoutException) {
				return true;
			} catch (InterruptedException e) {
				mismatchDescription.appendText("The waiting thread was interrupted.");
			} catch (ExecutionException e) {
				mismatchDescription.appendText("The given future was completed exceptionally: ")
						.appendValue(e);
			}

			return false;
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("The given future should not complete within ")
					.appendValue(timeout.toMillis())
					.appendText(" ms.");
		}
	}
}
