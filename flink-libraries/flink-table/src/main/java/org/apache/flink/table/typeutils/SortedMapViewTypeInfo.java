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

package org.apache.flink.table.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.SortedMapSerializer;
import org.apache.flink.table.api.dataview.SortedMapView;

import org.apache.flink.shaded.guava18.com.google.common.primitives.UnsignedBytes;

/**
 * Special {@code TypeInformation} used by {@link org.apache.flink.table.api.dataview.SortedMapView}.
 *
 * @param <K> The type of the keys in the map.
 * @param <V> The type of the values in the map.
 */
@PublicEvolving
public class SortedMapViewTypeInfo<K, V> extends TypeInformation<SortedMapView<K, V>> {

	/** The comparator for the keys in the map. */
	public final Comparator<K> comparator;
	public TypeInformation<K> keyType;
	public TypeInformation<V> valueType;

	public boolean nullSerializer = false;

	public SortedMapViewTypeInfo(
			Comparator<K> comparator,
			TypeInformation<K> keyType,
			TypeInformation<V> valueType) {
		this.comparator = comparator;
		this.keyType = keyType;
		this.valueType = valueType;
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<SortedMapView<K, V>> getTypeClass() {
		return (Class<SortedMapView<K, V>>) (Class<?>) SortedMapView.class;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<SortedMapView<K, V>> createSerializer(ExecutionConfig config) {
		if (nullSerializer) {
			return (TypeSerializer) new NullSerializer();
		}

		Comparator<K> keyComparaotr = comparator != null ? comparator : new SortedMapViewTypeInfo.ComparableComparator();

		TypeSerializer<K> keySer = keyType.createSerializer(config);

		TypeSerializer<V> valueSer = valueType.createSerializer(config);

		return new SortedMapViewSerializer<K, V>(new SortedMapSerializer<K, V>(
			keyComparaotr,
			keySer,
			valueSer));
	}

	@Override
	public String toString() {
		return "SortedMapView<" + keyType.toString() + ", " + valueType.toString() + ">";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj instanceof SortedMapViewTypeInfo) {
			@SuppressWarnings("unchecked")
			SortedMapViewTypeInfo<K, V> other = (SortedMapViewTypeInfo<K, V>) obj;
			return (other.canEqual(this) &&
					keyType.equals(other.keyType) &&
					valueType.equals(other.valueType) &&
					comparator.equals(other.comparator) &&
					nullSerializer == other.nullSerializer);

		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return 31 * 31 * keyType.hashCode() + 31 * valueType.hashCode() +
			Boolean.hashCode(nullSerializer);
	}

	@Override
	public boolean canEqual(Object obj) {
		return (obj != null && obj.getClass() == getClass());
	}

	public SortedMapViewTypeInfo<K, V> copy(boolean nullSerializer) {
		SortedMapViewTypeInfo<K, V> ret = new SortedMapViewTypeInfo<K, V>(comparator, keyType, valueType);
		ret.nullSerializer = nullSerializer;
		return ret;
	}

	/**
	 * The default comparator for comparable types.
	 */
	public static class ComparableComparator<K> implements Comparator<K>, java.io.Serializable {
		private static final long serialVersionUID = 1318143293435568038L;

		private boolean asc;

		public ComparableComparator() {
			this.asc = true;
		}

		public ComparableComparator(boolean asc) {
			this.asc = asc;
		}

		@Override
		public int compare(K o1, K o2) {
			int cmp = ((Comparable<K>) o1).compareTo(o2);
			return asc ? cmp : -cmp;
		}

		@Override
		public int hashCode() {
			return "ComparableComparator".hashCode();
		}

		@Override
		public boolean equals(Object object) {
			return this == object || null != object && object.getClass().equals(this.getClass());
		}
	}

	/**
	 * The comparator for byte array.
	 */
	public static class ByteArrayComparator implements Comparator<byte[]>, java.io.Serializable {
		private static final long serialVersionUID = 1318143293435568038L;

		private transient java.util.Comparator<byte[]> comparator;

		private boolean asc;

		public ByteArrayComparator() {
			this(true);
		}

		public ByteArrayComparator(boolean asc) {
			this.asc = asc;
			initComparator();
		}

		private void initComparator() {
			comparator = UnsignedBytes.lexicographicalComparator();
			if (!asc) {
				comparator = comparator.reversed();
			}
		}

		@Override
		public int compare(byte[] o1, byte[] o2) {
			if (comparator == null) {
				initComparator();
			}
			return comparator.compare(o1, o2);
		}

		@Override
		public int hashCode() {
			return "ByteArrayComparator".hashCode();
		}

		@Override
		public boolean equals(Object object) {
			return this == object || null != object && object.getClass().equals(this.getClass());
		}
	}
}
