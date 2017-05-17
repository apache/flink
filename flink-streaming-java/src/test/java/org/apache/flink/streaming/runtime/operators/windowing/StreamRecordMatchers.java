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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matchers that are useful for working with {@link StreamRecord StreamRecords}. This ...
 */
public class StreamRecordMatchers {

	public static <T> Matcher<StreamRecord<? extends T>> isStreamRecord(
		T value) {

		return isStreamRecord(Matchers.equalTo(value));
	}

	public static <T> Matcher<StreamRecord<? extends T>> isStreamRecord(
		T value,
		long timestamp) {

		return isStreamRecord(Matchers.equalTo(value), Matchers.equalTo(timestamp));
	}

	public static <T> Matcher<StreamRecord<? extends T>> isStreamRecord(
		Matcher<? super T> valueMatcher) {
		return new StreamRecordMatcher<>(valueMatcher, Matchers.anything());
	}

	public static <T> Matcher<StreamRecord<? extends T>> isStreamRecord(
		Matcher<? super T> valueMatcher, Matcher<? super Long> timestampMatcher) {
		return new StreamRecordMatcher<>(valueMatcher, timestampMatcher);
	}

	public static Matcher<TimeWindow> timeWindow(long start, long end) {
		return Matchers.equalTo(new TimeWindow(start, end));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@SafeVarargs
	public static <W extends Window> Matcher<Iterable<W>> ofWindows(Matcher<W>... windows) {
		return (Matcher) Matchers.containsInAnyOrder(windows);
	}

	public static <T, W extends Window> Matcher<StreamRecord<? extends WindowedValue<? extends T, ? extends W>>> isWindowedValue(
		T value) {
		return isWindowedValue(Matchers.equalTo(value));
	}

	public static <T, W extends Window> Matcher<StreamRecord<? extends WindowedValue<? extends T, ? extends W>>> isWindowedValue(
		T value,
		long timestamp) {
		return isWindowedValue(Matchers.equalTo(value), Matchers.equalTo(timestamp));
	}

	public static <T, W extends Window> Matcher<StreamRecord<? extends WindowedValue<? extends T, ? extends W>>> isWindowedValue(
		T value,
		long timestamp,
		W window) {
		return isWindowedValue(Matchers.equalTo(value), Matchers.equalTo(timestamp), Matchers.equalTo(window));
	}

	public static <T, W extends Window> Matcher<StreamRecord<? extends WindowedValue<? extends T, ? extends W>>> isWindowedValue(
		Matcher<? super T> valueMatcher, long timestamp) {
		return new WindowedValueMatcher<>(valueMatcher, Matchers.equalTo(timestamp), Matchers.anything());
	}

	public static <T, W extends Window> Matcher<StreamRecord<? extends WindowedValue<? extends T, ? extends W>>> isWindowedValue(
		Matcher<? super T> valueMatcher, long timestamp, W window) {
		return new WindowedValueMatcher<>(valueMatcher, Matchers.equalTo(timestamp), Matchers.equalTo(window));
	}

	public static <T, W extends Window> Matcher<StreamRecord<? extends WindowedValue<? extends T, ? extends W>>> isWindowedValue(
		Matcher<? super T> valueMatcher) {
		return new WindowedValueMatcher<>(valueMatcher, Matchers.anything(), Matchers.anything());
	}

	public static <T, W extends Window> Matcher<StreamRecord<? extends WindowedValue<? extends T, ? extends W>>> isWindowedValue(
		Matcher<? super T> valueMatcher, Matcher<? super Long> timestampMatcher) {
		return new WindowedValueMatcher<>(valueMatcher, timestampMatcher, Matchers.anything());
	}

	public static <T, W extends Window> Matcher<StreamRecord<? extends WindowedValue<? extends T, ? extends W>>> isWindowedValue(
		Matcher<? super T> valueMatcher, long timestamp, Matcher<? super W> windowMatcher) {
		return new WindowedValueMatcher<>(valueMatcher, Matchers.equalTo(timestamp), windowMatcher);
	}

	public static <T, W extends Window> Matcher<StreamRecord<? extends WindowedValue<? extends T, ? extends W>>> isWindowedValue(
		Matcher<? super T> valueMatcher, Matcher<? super Long> timestampMatcher, Matcher<? super W> windowMatcher) {
		return new WindowedValueMatcher<>(valueMatcher, timestampMatcher, windowMatcher);
	}

	private StreamRecordMatchers() {
	}

	private static class StreamRecordMatcher<T> extends TypeSafeMatcher<StreamRecord<? extends T>> {

		private Matcher<? super T> valueMatcher;
		private Matcher<? super Long> timestampMatcher;

		private StreamRecordMatcher(
			Matcher<? super T> valueMatcher,
			Matcher<? super Long> timestampMatcher) {
			this.valueMatcher = valueMatcher;
			this.timestampMatcher = timestampMatcher;
		}

		@Override
		public void describeTo(Description description) {
			description
				.appendText("a StreamRecordValue(").appendValue(valueMatcher)
				.appendText(", ").appendValue(timestampMatcher)
				.appendText(")");
		}

		@Override
		protected boolean matchesSafely(StreamRecord<? extends T> streamRecord) {
			return valueMatcher.matches(streamRecord.getValue())
				&& timestampMatcher.matches(streamRecord.getTimestamp());
		}
	}

	private static class WindowedValueMatcher<T, W extends Window> extends TypeSafeMatcher<StreamRecord<? extends WindowedValue<? extends T, ? extends W>>> {

		private Matcher<? super T> valueMatcher;
		private Matcher<? super Long> timestampMatcher;
		private Matcher<? super W> windowMatcher;

		private WindowedValueMatcher(
			Matcher<? super T> valueMatcher,
			Matcher<? super Long> timestampMatcher,
			Matcher<? super W> windowMatcher) {
			this.valueMatcher = valueMatcher;
			this.timestampMatcher = timestampMatcher;
			this.windowMatcher = windowMatcher;
		}

		@Override
		public void describeTo(Description description) {
			description
				.appendText("a WindowedValue(").appendValue(valueMatcher)
				.appendText(", ").appendValue(timestampMatcher)
				.appendText(", ").appendValue(timestampMatcher)
				.appendText(")");
		}

		@Override
		protected boolean matchesSafely(StreamRecord<? extends WindowedValue<? extends T, ? extends W>> streamRecord) {
			return valueMatcher.matches(streamRecord.getValue().value())
				&& timestampMatcher.matches(streamRecord.getTimestamp())
				&& windowMatcher.matches(streamRecord.getValue().window());
		}
	}
}
