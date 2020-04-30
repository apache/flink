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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Comparator;
import java.util.SortedMap;

/**
 * The type information for sorted maps.
 *
 * @param <K> The type of the keys in the map.
 * @param <V> The type of the values in the map.
 */
@PublicEvolving
public class SortedMapTypeInfo<K, V> extends AbstractMapTypeInfo<K, V, SortedMap<K, V>> {

	private static final long serialVersionUID = 1L;

	/** The comparator for the keys in the map. */
	private final Comparator<K> comparator;

	public SortedMapTypeInfo(
			TypeInformation<K> keyTypeInfo,
			TypeInformation<V> valueTypeInfo,
			Comparator<K> comparator) {
		super(keyTypeInfo, valueTypeInfo);

		Preconditions.checkNotNull(comparator, "The comparator cannot be null.");
		this.comparator = comparator;
	}

	public SortedMapTypeInfo(
			Class<K> keyClass,
			Class<V> valueClass,
			Comparator<K> comparator) {
		super(keyClass, valueClass);

		Preconditions.checkNotNull(comparator, "The comparator cannot be null.");
		this.comparator = comparator;
	}

	public SortedMapTypeInfo(Class<K> keyClass, Class<V> valueClass) {
		super(keyClass, valueClass);

		Preconditions.checkArgument(Comparable.class.isAssignableFrom(keyClass),
				"The key class must be comparable when no comparator is given.");
		this.comparator = new ComparableComparator<>();
	}

	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public Class<SortedMap<K, V>> getTypeClass() {
		return (Class<SortedMap<K, V>>) (Class<?>) SortedMap.class;
	}

	@Override
	public TypeSerializer<SortedMap<K, V>> createSerializer(ExecutionConfig config) {
		TypeSerializer<K> keyTypeSerializer = keyTypeInfo.createSerializer(config);
		TypeSerializer<V> valueTypeSerializer = valueTypeInfo.createSerializer(config);

		return new SortedMapSerializer<>(comparator, keyTypeSerializer, valueTypeSerializer);
	}

	@Override
	public boolean canEqual(Object obj) {
		return null != obj && getClass() == obj.getClass();
	}

	@Override
	public boolean equals(Object o) {
		if (!super.equals(o)) {
			return false;
		}

		SortedMapTypeInfo<?, ?> that = (SortedMapTypeInfo<?, ?>) o;

		return comparator.equals(that.comparator);
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + comparator.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "SortedMapTypeInfo{" +
				"comparator=" + comparator +
				", keyTypeInfo=" + getKeyTypeInfo() +
				", valueTypeInfo=" + getValueTypeInfo() +
				"}";
	}

	//--------------------------------------------------------------------------

	/**
	 * The default comparator for comparable types.
	 */
	private static class ComparableComparator<K> implements Comparator<K>, Serializable {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings("unchecked")
		public int compare(K obj1, K obj2) {
			return ((Comparable<K>) obj1).compareTo(obj2);
		}

		@Override
		public boolean equals(Object o) {
			return (o == this) || (o != null && o.getClass() == getClass());
		}

		@Override
		public int hashCode() {
			return "ComparableComparator".hashCode();
		}

	}
}
