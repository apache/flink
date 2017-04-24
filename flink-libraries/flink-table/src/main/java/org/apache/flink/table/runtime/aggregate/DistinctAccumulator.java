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
package org.apache.flink.table.runtime.aggregate;

import java.util.HashMap;

public class DistinctAccumulator<T> {
	private final HashMap<Object, Integer> counters = new HashMap<>();
	public final T delegate;

	public DistinctAccumulator(T delegate) {
		this.delegate = delegate;
	}

	/**
	 * Add an element into the counters.
	 * @param o value
	 * @return true if the delegated accumulator needs to be called.
	 */
	public boolean add(Object o) {
		if (counters.containsKey(o)) {
			counters.put(o, counters.get(o) + 1);
			return false;
		} else {
			counters.put(o, 1);
			return true;
		}
	}

	/**
	 * Retract an element from the counters.
	 * @param o value
	 * @return true if the delegated accumulator needs to be called.
	 */
	public boolean retract(Object o) {
		Integer v = counters.get(o);
		if (v == null) {
			return false;
		} else {
			--v;
			if (v == 0) {
				counters.remove(o);
				return true;
			}
			counters.put(o, v);
			return false;
		}
	}

	public void reset() {
		counters.clear();
	}

	public Iterable<Object> values() {
		return counters.keySet();
	}
}
