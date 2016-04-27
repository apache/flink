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

import java.util.Map;
import java.util.TreeMap;

/**
 * Histogram accumulator, which builds a histogram in a distributed manner.
 * Basically the same as {@link Histogram} with Long instead of Integer values.
 * Implemented as a Integer-&gt;Long TreeMap, so that the entries are sorted
 * according to the values.
 * 
 * This class does not extend to continuous values later, because it makes no
 * attempt to put the data in bins.
 */
@Public
public class LongHistogram implements Accumulator<Integer, TreeMap<Integer, Long>> {

	private static final long serialVersionUID = 1L;

	private TreeMap<Integer, Long> treeMap = new TreeMap<Integer, Long>();

	@Override
	public void add(Integer value) {
		Long current = treeMap.get(value);
		Long newValue = (current != null ? current : 0) + 1;
		this.treeMap.put(value, newValue);
	}

	@Override
	public TreeMap<Integer, Long> getLocalValue() {
		return this.treeMap;
	}

	@Override
	public void merge(Accumulator<Integer, TreeMap<Integer, Long>> other) {
		// Merge the values into this map
		for (Map.Entry<Integer, Long> entryFromOther : other.getLocalValue().entrySet()) {
			Long ownValue = this.treeMap.get(entryFromOther.getKey());
			if (ownValue == null) {
				this.treeMap.put(entryFromOther.getKey(), entryFromOther.getValue());
			} else {
				this.treeMap.put(entryFromOther.getKey(), entryFromOther.getValue() + ownValue);
			}
		}
	}

	@Override
	public void resetLocal() {
		this.treeMap.clear();
	}

	@Override
	public String toString() {
		return this.treeMap.toString();
	}

	@Override
	public Accumulator<Integer, TreeMap<Integer, Long>> clone() {
		LongHistogram result = new LongHistogram();
		result.treeMap = new TreeMap<Integer, Long>(treeMap);
		return result;
	}
}
