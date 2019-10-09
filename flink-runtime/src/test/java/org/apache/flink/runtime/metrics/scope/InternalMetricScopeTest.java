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

package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.DelimiterProvider;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link InternalMetricScope}.
 */
public class InternalMetricScopeTest extends TestLogger {

	private static final CharacterFilter FILTER_C = input -> input.replace("C", "X");

	@Test
	public void testScopeCachingForMultipleReporters() {
		InternalMetricScope scope = new InternalMetricScope(new ConsistentDelimiterProvider(2), new String[]{"A", "B", "C", "D"}, Collections::emptyMap);

		// the first call determines which filter is applied to all future calls; in this case no filter is used at all
		assertEquals("A.B.C.D.1", scope.getMetricIdentifier("1", s -> s, 0));
		// from now on the scope string is cached and should not be reliant on the given filter
		assertEquals("A.B.C.D.1", scope.getMetricIdentifier("1", FILTER_C, 0));
		// the metric name however is still affected by the filter as it is not cached
		assertEquals("A.B.C.D.4", scope.getMetricIdentifier("1", input -> input.replace("B", "X").replace("1", "4"), 0));

		// verify that caches are isolated between reporters
		assertEquals("A.B.X.D.1", scope.getMetricIdentifier("1", FILTER_C, 1));
	}

	@Test
	public void testScopeGenerationWithoutReporters() {
		InternalMetricScope scope = new InternalMetricScope(new ConsistentDelimiterProvider(0), new String[]{"A", "B", "C", "D"}, Collections::emptyMap);

		// default delimiter should be used
		assertEquals("A.B.C.D.1", scope.getMetricIdentifier("1"));
		// no caching should occur
		assertEquals("A.B.X.D.1", scope.getMetricIdentifier("1", FILTER_C));
		// invalid reporter indices do not throw errors
		assertEquals("A.B.X.D.1", scope.getMetricIdentifier("1", FILTER_C, -1));
		assertEquals("A.B.X.D.1", scope.getMetricIdentifier("1", FILTER_C, 2));
	}

	private static class ConsistentDelimiterProvider implements DelimiterProvider {
		private final int numReporters;

		private ConsistentDelimiterProvider(int numReporters) {
			this.numReporters = numReporters;
		}

		@Override
		public char getDelimiter() {
			return '.';
		}

		@Override
		public char getDelimiter(int index) {
			return getDelimiter();
		}

		@Override
		public int getNumberReporters() {
			return numReporters;
		}
	}
}
