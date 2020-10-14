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

package org.apache.flink.table.runtime.operators.window;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.Int2HashJoinOperatorTest;
import org.apache.flink.table.runtime.operators.window.assigners.SessionWindowAssigner;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.Collections;

/**
 * Utilities that are useful for working with Window tests.
 */
public interface WindowTestUtils {

	static Matcher<TimeWindow> timeWindow(long start, long end) {
		return Matchers.equalTo(new TimeWindow(start, end));
	}

	static GeneratedJoinCondition alwaysTrueCondition() {
		return new GeneratedJoinCondition("", "", new Object[0]) {
			private static final long serialVersionUID = 1L;

			@Override
			public JoinCondition newInstance(ClassLoader classLoader) {
				return new Int2HashJoinOperatorTest.TrueCondition();
			}
		};
	}

	/** A special session window assigner that assigns "point windows"
	 * (windows that have the same timestamp for start and end)
	 * when the given {@code targetRowIdx} field has the expected value {@code expectVal}.
	 *
	 * <p>The {@code expectVal} should be an integer.
	 */
	class PointSessionWindowAssigner extends SessionWindowAssigner {
		private static final long serialVersionUID = 1L;

		private final long sessionTimeout;
		private final int targetRowIdx;
		private final int expectVal;

		public static PointSessionWindowAssigner of(long sessionTimeout, int targetRowIdx, int expectVal) {
			return new PointSessionWindowAssigner(sessionTimeout, targetRowIdx, expectVal);
		}

		private PointSessionWindowAssigner(long sessionTimeout, int targetRowIdx, int expectVal) {
			this(sessionTimeout, true, targetRowIdx, expectVal);
		}

		private PointSessionWindowAssigner(
				long sessionTimeout,
				boolean isEventTime,
				int targetRowIdx,
				int expectVal) {
			super(sessionTimeout, isEventTime);
			this.sessionTimeout = sessionTimeout;
			this.targetRowIdx = targetRowIdx;
			this.expectVal = expectVal;
		}

		@Override
		public Collection<TimeWindow> assignWindows(RowData element, long timestamp) {
			int second = element.getInt(targetRowIdx);
			if (second == expectVal) {
				return Collections.singletonList(new TimeWindow(timestamp, timestamp));
			}
			return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
		}

		@Override
		public SessionWindowAssigner withEventTime() {
			return new PointSessionWindowAssigner(
					sessionTimeout, true, targetRowIdx, expectVal);
		}

		@Override
		public SessionWindowAssigner withProcessingTime() {
			return new PointSessionWindowAssigner(
					sessionTimeout, false, targetRowIdx, expectVal);
		}
	}
}
