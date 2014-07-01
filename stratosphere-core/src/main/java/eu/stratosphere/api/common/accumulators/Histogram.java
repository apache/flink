/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.common.accumulators;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

/**
 * Histogram for discrete-data. Let's you populate a histogram distributedly.
 * Implemented as a Integer->Integer HashMap
 * 
 * Could be extended to continuous values later, but then we need to dynamically
 * decide about the bin size in an online algorithm (or ask the user)
 */
public class Histogram implements Accumulator<Integer, Map<Integer, Integer>> {

	private static final long serialVersionUID = 1L;

	private Map<Integer, Integer> hashMap = Maps.newHashMap();

	@Override
	public void add(Integer value) {
		Integer current = hashMap.get(value);
		Integer newValue = value;
		if (current != null) {
			newValue = current + newValue;
		}
		this.hashMap.put(value, newValue);
	}

	@Override
	public Map<Integer, Integer> getLocalValue() {
		return this.hashMap;
	}

	@Override
	public void merge(Accumulator<Integer, Map<Integer, Integer>> other) {
		// Merge the values into this map
		for (Map.Entry<Integer, Integer> entryFromOther : ((Histogram) other).getLocalValue()
				.entrySet()) {
			Integer ownValue = this.hashMap.get(entryFromOther.getKey());
			if (ownValue == null) {
				this.hashMap.put(entryFromOther.getKey(), entryFromOther.getValue());
			} else {
				this.hashMap.put(entryFromOther.getKey(), entryFromOther.getValue() + ownValue);
			}
		}
	}

	@Override
	public void resetLocal() {
		this.hashMap.clear();
	}

	@Override
	public String toString() {
		return this.hashMap.toString();
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(hashMap.size());
		for (Map.Entry<Integer, Integer> entry : hashMap.entrySet()) {
			out.writeInt(entry.getKey());
			out.writeInt(entry.getValue());
		}
	}

	@Override
	public void read(DataInputView in) throws IOException {
		int size = in.readInt();
		for (int i = 0; i < size; ++i) {
			hashMap.put(in.readInt(), in.readInt());
		}
	}

}
