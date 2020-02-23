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
package org.apache.flink.runtime.executiongraph.failover.flip1;

import com.esotericsoftware.kryo.util.IdentityObjectIntMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;


public class DisjointSet<V> {
	private Object[] elements;
	private int[] toSet;
	private int[] stack;
	private int[] ranks;
	private IdentityObjectIntMap<Object> indices;
	public DisjointSet(Iterable<V> elements) {
		this.elements = Iterables.toArray(elements, Object.class);
		this.toSet = new int[this.elements.length];
		this.stack = new int[this.elements.length];
		this.ranks = new int[this.elements.length];
		this.indices = new IdentityObjectIntMap<>(this.elements.length);
		for (int i = 0; i < this.elements.length; i++) {
			this.indices.put(this.elements[i], i);
			this.toSet[i] = i;
			this.ranks[i] = 1;
		}
	}

	public int mergeSet(int from, int to) {
		if (from == to) {
			return to;
		}
		if (ranks[from] > ranks[to]) {
			// switch from, to if from's rank is bigger than to
			int tmp;
			tmp = from;
			from = to;
			to = tmp;
		}
		toSet[from] = to;
		ranks[to] += ranks[from];
		return to;
	}

	public int getSetByIndex(int e) {
		int set = toSet[e];
		if (set == e) {
			return e;
		}
		int stackIndex = 0;
		do {
			stack[stackIndex] = e;
			e = set;
			stackIndex += 1;
			set = toSet[set];
		} while (e != set);
		for (int i = 0; i < stackIndex; i++) {
			toSet[stack[i]] = set;
		}
		return set;
	}

	public int getSet(V e) {
		int index = indices.get(e, -1);
		return getSetByIndex(index);
	}

	public Set<Set<V>> toSets() {
		Set[] sets = new Set[this.elements.length];
		Set<Set<V>> ret = Collections.newSetFromMap(new IdentityHashMap<>());
		for (int i = 0; i < this.elements.length; i++) {
			int set = getSetByIndex(i);
			if (sets[set] == null) {
				sets[set] = Collections.newSetFromMap(new IdentityHashMap<>(ranks[set]));
				ret.add(sets[set]);
			}
			sets[set].add(this.elements[i]);
		}
		return ret;
	}
}
