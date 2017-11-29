/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Predicate;

/**
 * @param <T> type of the contained state objects.
 */
public class StateObjectCollection<T extends StateObject> implements Collection<T>, StateObject {

	private static final long serialVersionUID = 1L;

	private static final StateObjectCollection<?> EMPTY = new StateObjectCollection<>(Collections.emptyList());
	private final Collection<T> stateObjects;

	public StateObjectCollection() {
		this.stateObjects = new ArrayList<>();
	}

	public StateObjectCollection(Collection<T> stateObjects) {
		this.stateObjects = stateObjects != null ? stateObjects : Collections.emptyList();
	}

	@Override
	public int size() {
		return stateObjects.size();
	}

	@Override
	public boolean isEmpty() {
		return stateObjects.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return stateObjects.contains(o);
	}

	@Override
	public Iterator<T> iterator() {
		return stateObjects.iterator();
	}

	@Override
	public Object[] toArray() {
		return stateObjects.toArray();
	}

	@Override
	public <T1> T1[] toArray(T1[] a) {
		return stateObjects.toArray(a);
	}

	@Override
	public boolean add(T t) {
		return stateObjects.add(t);
	}

	@Override
	public boolean remove(Object o) {
		return stateObjects.remove(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return stateObjects.containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		return stateObjects.addAll(c);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return stateObjects.removeAll(c);
	}

	@Override
	public boolean removeIf(Predicate<? super T> filter) {
		return stateObjects.removeIf(filter);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return stateObjects.retainAll(c);
	}

	@Override
	public void clear() {
		stateObjects.clear();
	}

	private static long sumAllSizes(Collection<? extends StateObject> stateObject) {
		long size = 0L;
		for (StateObject object : stateObject) {
			size += getSizeNullSafe(object);
		}

		return size;
	}

	private static long getSizeNullSafe(StateObject stateObject) {
		return stateObject != null ? stateObject.getStateSize() : 0L;
	}

	@Override
	public void discardState() throws Exception {
		StateUtil.bestEffortDiscardAllStateObjects(stateObjects);
	}

	@Override
	public long getStateSize() {
		return sumAllSizes(stateObjects);
	}

	public boolean hasState() {
		for (StateObject state : stateObjects) {
			if (state != null) {
				return true;
			}
		}
		return false;
	}

	public static <T extends StateObject> StateObjectCollection<T> empty() {
		return (StateObjectCollection<T>) EMPTY;
	}

	public static <T extends StateObject> StateObjectCollection<T> singleton(T stateObject) {
		return new StateObjectCollection<>(Collections.singleton(stateObject));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		StateObjectCollection<?> that = (StateObjectCollection<?>) o;

		// simple equals can cause troubles here because of how equals works e.g. between lists and sets.
		return CollectionUtils.isEqualCollection(stateObjects, that.stateObjects);
	}

	@Override
	public int hashCode() {
		return stateObjects.hashCode();
	}

	@Override
	public String toString() {
		return "StateObjectCollection{" + stateObjects + '}';
	}
}
