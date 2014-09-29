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

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Histogram for discrete-data. Let's you populate a histogram distributedly.
 * Implemented as a Integer->Integer TreeMap, so that the entries are sorted
 * according to the values.
 * 
 * Could be extended to continuous values later, but then we need to dynamically
 * decide about the bin size in an online algorithm (or ask the user)
 */
public class Histogram implements Accumulator<Integer, Map<Integer, Integer>> {

	private static final long serialVersionUID = 1L;

	private Map<Integer, Integer> treeMap = new TreeMap<Integer, Integer>();

	@Override
	public void add(Integer value) {
		Integer current = treeMap.get(value);
		Integer newValue = (current != null ? current : 0) + 1;
		this.treeMap.put(value, newValue);
	}

	@Override
	public Map<Integer, Integer> getLocalValue() {
		return this.treeMap;
	}

	@Override
	public void merge(Accumulator<Integer, Map<Integer, Integer>> other) {
		// Merge the values into this map
		for (Map.Entry<Integer, Integer> entryFromOther : ((Histogram) other).getLocalValue()
				.entrySet()) {
			Integer ownValue = this.treeMap.get(entryFromOther.getKey());
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
	public void write(DataOutputView out) throws IOException {
		out.writeInt(treeMap.size());
		for (Map.Entry<Integer, Integer> entry : treeMap.entrySet()) {
			out.writeInt(entry.getKey());
			out.writeInt(entry.getValue());
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		int size = in.readInt();
		for (int i = 0; i < size; ++i) {
			treeMap.put(in.readInt(), in.readInt());
		}
	}

}
