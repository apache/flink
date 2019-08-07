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

package org.apache.flink.table.planner.functions.aggfunctions.hll;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.planner.functions.aggfunctions.ApproximateCountDistinctAggFunction.HllAccumulator;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link HyperLogLogPlusPlus}.
 */
public class HyperLogLogPlusPlusTest {

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidRelativeSD() {
		// `relativeSD` should be at most 39%.
		new HyperLogLogPlusPlus(0.4);
	}

	@Test
	public void testAddNulls() {
		Tuple3<HyperLogLogPlusPlus, GenericRow, HllAccumulator> tuple3 = createEstimator(0.05);
		HyperLogLogPlusPlus hll = tuple3.f0;
		GenericRow input = tuple3.f1;
		HllAccumulator accumulator = tuple3.f2;
		input.setNullAt(0);
		hll.update(accumulator, input.getField(0));
		hll.update(accumulator, input.getField(0));
		long estimate = hll.query(accumulator);
		assertEquals(0L, estimate);
	}

	@Test
	public void testDeterministicCardinalityEstimates() {
		int repeats = 10;
		testCardinalityEstimates(
				Arrays.asList(0.1, 0.05, 0.025, 0.01, 0.001),
				Stream.of(100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000)
						.map(n -> n * repeats).collect(Collectors.toList()),
				i -> i / repeats,
				i -> i / repeats);
	}

	@Test
	public void testRandomCardinalityEstimates() {
		Random srng = new Random(323981238L);
		Set<Integer> seen = new HashSet<>();

		testCardinalityEstimates(
				Arrays.asList(0.05, 0.01),
				Arrays.asList(100, 10000, 500000),
				i -> {
					int value = srng.nextInt();
					seen.add(value);
					return value;
				},
				i -> {
					int cardinality = seen.size();
					seen.clear();
					return cardinality;
				});
	}

	@Test
	public void testMerging() {
		Tuple3<HyperLogLogPlusPlus, GenericRow, HllAccumulator> tuple3 = createEstimator(0.05);
		HyperLogLogPlusPlus hll = tuple3.f0;
		GenericRow input = tuple3.f1;
		HllAccumulator acc1a = tuple3.f2;
		HllAccumulator acc1b = createAccumulator(hll);
		HllAccumulator acc2 = createAccumulator(hll);

		// Add the lower half.
		for (int i = 0; i < 500000; ++i) {
			input.setInt(0, i);
			hll.update(acc1a, input.getField(0));
		}

		// Add the upper half.
		for (int i = 500000; i < 1000000; ++i) {
			input.setInt(0, i);
			hll.update(acc1b, input.getField(0));
		}

		// Merge the lower and upper halves.
		hll.merge(acc1a, acc1b);

		// Create the other accumulator in reverse
		for (int i = 999999; i >= 0; i--) {
			input.setInt(0, i);
			hll.update(acc2, input.getField(0));
		}

		// Check if the accumulators are equal.
		assertArrayEquals(acc2.array, acc1a.array);
	}

	private void testCardinalityEstimates(
			List<Double> rsds,
			List<Integer> ns,
			Function<Integer, Integer> f,
			Function<Integer, Integer> c) {
		rsds.stream().flatMap(rsd -> {
			return ns.stream().map(n -> new Tuple2<>(rsd, n)).collect(Collectors.toList()).stream();
		}).forEach(tuple2 -> {
			double rsd = tuple2.f0;
			int n = tuple2.f1;
			Tuple3<HyperLogLogPlusPlus, GenericRow, HllAccumulator> tuple3 = createEstimator(rsd);
			HyperLogLogPlusPlus hll = tuple3.f0;
			GenericRow input = tuple3.f1;
			HllAccumulator accumulator = tuple3.f2;

			for (int i = 0; i < n; ++i) {
				input.setInt(0, f.apply(i));
				hll.update(accumulator, input.getField(0));
			}

			double estimate = hll.query(accumulator);
			int cardinality = c.apply(n);
			double error = Math.abs((estimate * 1.0 / cardinality) - 1.0d);
			assertTrue(error < hll.trueRsd() * 3.0d);
		});
	}

	/** Create a HLL++ instance and an input and output accumulator. */
	private Tuple3<HyperLogLogPlusPlus, GenericRow, HllAccumulator> createEstimator(double rsd) {
		GenericRow input = new GenericRow(1);
		HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(rsd);
		HllAccumulator accumulator = createAccumulator(hll);
		return new Tuple3<>(hll, input, accumulator);
	}

	private HllAccumulator createAccumulator(HyperLogLogPlusPlus hll) {
		int numWords = hll.getNumWords();
		HllAccumulator accumulator = new HllAccumulator();
		accumulator.array = new long[numWords];

		int word = 0;
		while (word < numWords) {
			accumulator.array[word] = 0;
			word += 1;
		}

		return accumulator;
	}

}
