/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Hamcrest matchers.
 */
public class CoreMatchers {

	/**
	 * Checks for a {@link Throwable} that matches by class and message.
	 */
	public static Matcher<Throwable> containsCause(Throwable failureCause) {
		return new ContainsCauseMatcher(failureCause);
	}

	/**
	 * Checks that a {@link CompletableFuture} won't complete within the given timeout.
	 */
	public static Matcher<CompletableFuture<?>> willNotComplete(Duration timeout) {
		return new WillNotCompleteMatcher(timeout);
	}

	private static class ContainsCauseMatcher extends TypeSafeDiagnosingMatcher<Throwable> {

		private final Throwable failureCause;

		private ContainsCauseMatcher(Throwable failureCause) {
			this.failureCause = failureCause;
		}

		@Override
		protected boolean matchesSafely(Throwable throwable, Description description) {
			final Optional<Throwable> optionalCause = ExceptionUtils.findThrowable(
				throwable,
				cause -> cause.getClass() == failureCause.getClass() &&
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

	private CoreMatchers() {
		throw new UnsupportedOperationException("Cannot instantiate an instance of this class.");
	}
}
