/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.ttl;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/**
 * {@link Matcher Matchers} for {@link TtlValue}.
 */
public class TtlValueMatchers {

	/**
	 * Creates a matcher that matches when the given value and timestamp matchers match the value
	 * and timestamp in a {@link TtlValue};
	 */
	public static <T> Matcher<TtlValue<T>> ttlValue(
			Matcher<T> valueMatcher,
			Matcher<Long> timestampMatcher) {
		return new IsTtlValue<>(valueMatcher, timestampMatcher);
	}

	static class IsTtlValue<T> extends TypeSafeDiagnosingMatcher<TtlValue<T>> {
		private final Matcher<T> valueMatcher;
		private final Matcher<Long> timestampMatcher;

		public IsTtlValue(Matcher<T> valueMatcher, Matcher<Long> timestampMatcher) {
			this.valueMatcher = valueMatcher;
			this.timestampMatcher = timestampMatcher;
		}

		@Override
		protected boolean matchesSafely(TtlValue<T> item, Description mismatchDescription) {
			mismatchDescription.appendText("TtlValue with value ");
			mismatchDescription.appendValue(item.getUserValue());
			mismatchDescription.appendText(" with timestamp ");
			mismatchDescription.appendValue(item.getLastAccessTimestamp());
			return valueMatcher.matches(item.getUserValue()) &&
					timestampMatcher.matches(item.getLastAccessTimestamp());
		}

		@Override
		public void describeTo(Description description) {
			description.appendText("TtlValue with value ");
			valueMatcher.describeTo(description);
			description.appendText(" with timestamp ");
			timestampMatcher.describeTo(description);
		}
	}
}
