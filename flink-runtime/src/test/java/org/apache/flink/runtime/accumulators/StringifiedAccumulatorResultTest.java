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

package org.apache.flink.runtime.accumulators;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OptionalFailure;

import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link StringifiedAccumulatorResult}.
 */
public class StringifiedAccumulatorResultTest {

	@Test
	public void testSerialization() throws IOException {
		final String name = "a";
		final String type = "b";
		final String value = "c";
		final StringifiedAccumulatorResult original = new StringifiedAccumulatorResult(name, type, value);

		// Confirm no funny business in the constructor to getter pathway
		assertEquals(name, original.getName());
		assertEquals(type, original.getType());
		assertEquals(value, original.getValue());

		final StringifiedAccumulatorResult copy = CommonTestUtils.createCopySerializable(original);

		// Copy should have equivalent core fields
		assertEquals(name, copy.getName());
		assertEquals(type, copy.getType());
		assertEquals(value, copy.getValue());
	}

	@Test
	public void stringifyingResultsShouldIncorporateAccumulatorLocalValueDirectly() {
		final String name = "a";
		final int targetValue = 314159;
		final IntCounter acc = new IntCounter();
		acc.add(targetValue);
		final Map<String, OptionalFailure<Accumulator<?, ?>>> accumulatorMap = new HashMap<>();
		accumulatorMap.put(name, OptionalFailure.of(acc));

		final StringifiedAccumulatorResult[] results = StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulatorMap);

		assertEquals(1, results.length);

		final StringifiedAccumulatorResult firstResult = results[0];
		assertEquals(name, firstResult.getName());
		assertEquals("IntCounter", firstResult.getType());
		assertEquals(Integer.toString(targetValue), firstResult.getValue());
	}

	@Test
	public void stringifyingResultsShouldReportNullLocalValueAsNonnullValueString() {
		final String name = "a";
		final NullBearingAccumulator acc = new NullBearingAccumulator();
		final Map<String, OptionalFailure<Accumulator<?, ?>>> accumulatorMap = new HashMap<>();
		accumulatorMap.put(name, OptionalFailure.of(acc));

		final StringifiedAccumulatorResult[] results = StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulatorMap);

		assertEquals(1, results.length);

		// Note the use of a String with a content of "null" rather than a null value
		final StringifiedAccumulatorResult firstResult = results[0];
		assertEquals(name, firstResult.getName());
		assertEquals("NullBearingAccumulator", firstResult.getType());
		assertEquals("null", firstResult.getValue());
	}

	@Test
	public void stringifyingResultsShouldReportNullAccumulatorWithNonnullValueAndTypeString() {
		final String name = "a";
		final Map<String, OptionalFailure<Accumulator<?, ?>>> accumulatorMap = new HashMap<>();
		accumulatorMap.put(name, null);

		final StringifiedAccumulatorResult[] results = StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulatorMap);

		assertEquals(1, results.length);

		// Note the use of String values with content of "null" rather than null values
		final StringifiedAccumulatorResult firstResult = results[0];
		assertEquals(name, firstResult.getName());
		assertEquals("null", firstResult.getType());
		assertEquals("null", firstResult.getValue());
	}

	@Test
	public void stringifyingFailureResults() {
		final String name = "a";
		final Map<String, OptionalFailure<Accumulator<?, ?>>> accumulatorMap = new HashMap<>();
		accumulatorMap.put(name, OptionalFailure.ofFailure(new FlinkRuntimeException("Test")));

		final StringifiedAccumulatorResult[] results = StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulatorMap);

		assertEquals(1, results.length);

		// Note the use of String values with content of "null" rather than null values
		final StringifiedAccumulatorResult firstResult = results[0];
		assertEquals(name, firstResult.getName());
		assertEquals("null", firstResult.getType());
		assertTrue(firstResult.getValue().startsWith("org.apache.flink.util.FlinkRuntimeException: Test"));
	}

	private static class NullBearingAccumulator implements SimpleAccumulator<Serializable> {

		@Override
		public void add(Serializable value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Serializable getLocalValue() {
			return null;
		}

		@Override
		public void resetLocal() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void merge(Accumulator<Serializable, Serializable> other) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Accumulator<Serializable, Serializable> clone() {
			return new NullBearingAccumulator();
		}
	}
}
