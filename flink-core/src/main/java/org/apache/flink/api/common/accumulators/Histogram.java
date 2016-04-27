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

package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.MathUtils;

import java.util.Map;
import java.util.TreeMap;

/**
 * Histogram accumulator, which builds a histogram in a distributed manner.
 * Implemented as a Integer-&gt;Integer TreeMap, so that the entries are sorted
 * according to the values.
 *
 * This class does not extend to continuous values later, because it makes no
 * attempt to put the data in bins.
 */
@Public
public class Histogram implements Accumulator<Integer, TreeMap<Integer, Integer>> {

	private static final long serialVersionUID = 1L;

	private LongHistogram internalHistogram = new LongHistogram();

	@Override
	public void add(Integer value) {
		internalHistogram.add(value);
	}

	@Override
	public TreeMap<Integer, Integer> getLocalValue() {
		final TreeMap<Integer, Long> longTreeMap = internalHistogram.getLocalValue();
		return convertToIntMap(longTreeMap);
	}

	private TreeMap<Integer, Integer> convertToIntMap(final TreeMap<Integer, Long> longTreeMap) {
		final TreeMap<Integer, Integer> intTreeMap = new TreeMap<>();
		for (final Map.Entry<Integer, Long> entry : longTreeMap.entrySet()) {
			intTreeMap.put(entry.getKey(), checkedCast(entry.getValue()));
		}
		return intTreeMap;
	}

	private int checkedCast(final Long l) {
		try {
			return MathUtils.checkedDownCast(l);
		} catch (final IllegalArgumentException e) {
			throw new IllegalArgumentException("Histogram can only deal with int values, consider using LongHistogram.", e);
		}
	}

	@Override
	public void merge(Accumulator<Integer, TreeMap<Integer, Integer>> other) {
		// Merge the values into this map
		for (Map.Entry<Integer, Integer> entryFromOther : other.getLocalValue().entrySet()) {
			Long ownValue = internalHistogram.getLocalValue().get(entryFromOther.getKey());
			if (ownValue == null) {
				internalHistogram.getLocalValue().put(entryFromOther.getKey(), entryFromOther.getValue().longValue());
			} else {
				internalHistogram.getLocalValue().put(entryFromOther.getKey(), entryFromOther.getValue() + ownValue);
			}
		}
	}

	@Override
	public void resetLocal() {
		internalHistogram.resetLocal();
	}

	@Override
	public String toString() {
		return internalHistogram.toString();
	}

	@Override
	public Accumulator<Integer, TreeMap<Integer, Integer>> clone() {
		final Histogram result = new Histogram();
		result.internalHistogram = internalHistogram;
		return result;
	}
}
