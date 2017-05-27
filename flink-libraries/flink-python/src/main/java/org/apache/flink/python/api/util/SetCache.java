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

package org.apache.flink.python.api.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.UnsortedGrouping;

import java.util.HashMap;
import java.util.Map;

/**
 * A container holding {@link DataSet DataSets}, {@link SortedGrouping sorted} and{@link UnsortedGrouping unsorted}
 * groupings.
 */
public class SetCache {

	private enum SetType {

		DATA_SET(DataSet.class.getName()),
		UNSORTED_GROUPING(UnsortedGrouping.class.getName()),
		SORTED_GROUPING(SortedGrouping.class.getName());

		private final String className;

		SetType(String className) {
			this.className = className;
		}

		@Override
		public String toString() {
			return className;
		}
	}

	private final Map<Integer, SetType> setTypes = new HashMap<>();

	@SuppressWarnings("rawtypes")
	private final Map<Integer, DataSet> dataSets = new HashMap<>();
	@SuppressWarnings("rawtypes")
	private final Map<Integer, UnsortedGrouping> unsortedGroupings = new HashMap<>();
	@SuppressWarnings("rawtypes")
	private final Map<Integer, SortedGrouping> sortedGroupings = new HashMap<>();

	private int cachedID = -1;
	private SetType cachedType = null;

	/**
	 * Adds the given {@link DataSet} to this cache for the given ID.
	 *
	 * @param id  Set ID
	 * @param set DataSet to add
	 * @param <D> DataSet class
	 */
	public <D extends DataSet<?>> void add(int id, D set) {
		cacheSetType(id, SetType.DATA_SET);
		dataSets.put(id, set);
	}

	/**
	 * Adds the given {@link UnsortedGrouping} to this cache for the given ID.
	 *
	 * @param id  Set ID
	 * @param set UnsortedGrouping to add
	 * @param <U> UnsortedGrouping class
	 */
	public <U extends UnsortedGrouping<?>> void add(int id, U set) {
		cacheSetType(id, SetType.UNSORTED_GROUPING);
		unsortedGroupings.put(id, set);
	}

	/**
	 * Adds the given {@link SortedGrouping} to this cache for the given ID.
	 *
	 * @param id  Set ID
	 * @param set SortedGrouping to add
	 * @param <S> SortedGrouping class
	 */
	public <S extends SortedGrouping<?>> void add(int id, S set) {
		cacheSetType(id, SetType.SORTED_GROUPING);
		sortedGroupings.put(id, set);
	}

	private <T> void cacheSetType(int id, SetType type) {
		SetType prior = setTypes.put(id, type);
		if (prior != null) {
			throw new IllegalStateException("Set ID " + id + " used to denote multiple sets.");
		}
	}

	/**
	 * Checks whether the cached set for the given ID is a {@link DataSet}.
	 *
	 * @param id id of set to check
	 * @return true, if the cached set is a DataSet, false otherwise
	 */
	public boolean isDataSet(int id) {
		return isType(id, SetType.DATA_SET);
	}

	/**
	 * Checks whether the cached set for the given ID is an {@link UnsortedGrouping}.
	 *
	 * @param id id of set to check
	 * @return true, if the cached set is an UnsortedGrouping, false otherwise
	 */
	public boolean isUnsortedGrouping(int id) {
		return isType(id, SetType.UNSORTED_GROUPING);
	}

	/**
	 * Checks whether the cached set for the given ID is a {@link SortedGrouping}.
	 *
	 * @param id Set ID
	 * @return true, if the cached set is a SortedGrouping, false otherwise
	 */
	public boolean isSortedGrouping(int id) {
		return isType(id, SetType.SORTED_GROUPING);
	}

	private boolean isType(int id, SetType type) {
		if (cachedID != id) {
			cachedID = id;
			cachedType = setTypes.get(id);
			if (cachedType == null) {
				throw new IllegalStateException("No set exists for the given ID " + id);
			}
		}
		return cachedType == type;
	}

	/**
	 * Returns the cached {@link DataSet} for the given ID.
	 *
	 * @param id  Set ID
	 * @param <T> DataSet type
	 * @return Cached DataSet
	 * @throws IllegalStateException if the cached set is not a DataSet
	 */
	@SuppressWarnings("unchecked")
	public <T> DataSet<T> getDataSet(int id) {
		return verifyType(id, dataSets.get(id), SetType.DATA_SET);
	}

	/**
	 * Returns the cached {@link UnsortedGrouping} for the given ID.
	 *
	 * @param id  Set ID
	 * @param <T> UnsortedGrouping type
	 * @return Cached UnsortedGrouping
	 * @throws IllegalStateException if the cached set is not an UnsortedGrouping
	 */
	@SuppressWarnings("unchecked")
	public <T> UnsortedGrouping<T> getUnsortedGrouping(int id) {
		return verifyType(id, unsortedGroupings.get(id), SetType.UNSORTED_GROUPING);
	}

	/**
	 * Returns the cached {@link SortedGrouping} for the given ID.
	 *
	 * @param id  Set ID
	 * @param <T> SortedGrouping type
	 * @return Cached SortedGrouping
	 * @throws IllegalStateException if the cached set is not a SortedGrouping
	 */
	@SuppressWarnings("unchecked")
	public <T> SortedGrouping<T> getSortedGrouping(int id) {
		return verifyType(id, sortedGroupings.get(id), SetType.SORTED_GROUPING);
	}

	private <X> X verifyType(int id, X set, SetType type) {
		if (set == null) {
			SetType actualType = setTypes.get(id);
			throw new IllegalStateException("Set ID " + id + " did not denote a " + type + ", but a " + actualType + " instead.");
		}
		return set;
	}

	/**
	 * Resets this SetCache, removing any cached sets.
	 */
	public void reset() {
		setTypes.clear();

		dataSets.clear();
		unsortedGroupings.clear();
		sortedGroupings.clear();
	}
}
