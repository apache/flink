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

import java.util.Optional;

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

	private CoreMatchers() {
		throw new UnsupportedOperationException("Cannot instantiate an instance of this class.");
	}
}
