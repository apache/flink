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
package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Null-aware comparator that wraps a comparator which does not support null references.
 * <p>
 * NOTE: This class assumes to be used within a composite type comparator (such
 * as {@link RowComparator}) that handles serialized comparison.
 */
@Internal
public class NullAwareComparator<T> extends TypeComparator<T> {
	private static final long serialVersionUID = 1L;

	private final TypeComparator<T> wrappedComparator;
	private final boolean order;

	// number of flat fields
	private final int flatFields;

	// stores the null for reference comparison
	private boolean nullReference = false;

	public NullAwareComparator(TypeComparator<T> wrappedComparator, boolean order) {
		this.wrappedComparator = wrappedComparator;
		this.order = order;
		this.flatFields = wrappedComparator.getFlatComparators().length;
	}

	@Override
	public int hash(T record) {
		if (record != null) {
			return wrappedComparator.hash(record);
		} else {
			return 0;
		}
	}

	@Override
	public void setReference(T toCompare) {
		if (toCompare == null) {
			nullReference = true;
		} else {
			nullReference = false;
			wrappedComparator.setReference(toCompare);
		}
	}

	@Override
	public boolean equalToReference(T candidate) {
		// both values are null
		if (candidate == null && nullReference) {
			return true;
		}
		// one value is null
		else if (candidate == null || nullReference) {
			return false;
		}
		// no null value
		else {
			return wrappedComparator.equalToReference(candidate);
		}
	}

	@Override
	public int compareToReference(TypeComparator<T> referencedComparator) {
		NullAwareComparator otherComparator = (NullAwareComparator) referencedComparator;
		boolean otherNullReference = otherComparator.nullReference;
		// both values are null -> equality
		if (nullReference && otherNullReference) {
			return 0;
		}
		// first value is null -> inequality
		// but order is considered
		else if (nullReference) {
			return order ? 1 : -1;
		}
		// second value is null -> inequality
		// but order is considered
		else if (otherNullReference) {
			return order ? -1 : 1;
		}
		// no null values
		else {
			return wrappedComparator.compareToReference(otherComparator.wrappedComparator);
		}
	}

	@Override
	public int compare(T first, T second) {
		// both values are null -> equality
		if (first == null && second == null) {
			return 0;
		}
		// first value is null -> inequality
		// but order is considered
		else if (first == null) {
			return order ? -1 : 1;
		}
		// second value is null -> inequality
		// but order is considered
		else if (second == null) {
			return order ? 1 : -1;
		}
		// no null values
		else {
			return wrappedComparator.compare(first, second);
		}
	}

	@Override
	public int compareSerialized(
		DataInputView firstSource,
		DataInputView secondSource) throws IOException {

		throw new UnsupportedOperationException(
			"Comparator does not support null-aware serialized comparision.");
	}

	@Override
	public boolean supportsNormalizedKey() {
		return wrappedComparator.supportsNormalizedKey();
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		int len = wrappedComparator.getNormalizeKeyLen();
		if (len == Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		} else {
			// add one for a null byte
			return len + 1;
		}
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return wrappedComparator.isNormalizedKeyPrefixOnly(keyBytes - 1);
	}

	@Override
	public void putNormalizedKey(T record, MemorySegment target, int offset, int numBytes) {
		if (numBytes > 0) {
			// write a null byte with padding
			if (record == null) {
				target.putBoolean(offset, false);
				// write padding
				for (int j = 0; j < numBytes - 1; j++) {
					target.put(offset + 1 + j, (byte) 0);
				}
			}
			// write a non-null byte with key
			else {
				target.putBoolean(offset, true);
				// write key
				wrappedComparator.putNormalizedKey(record, target, offset + 1, numBytes - 1);
			}
		}
	}

	@Override
	public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException(
			"Record serialization with leading normalized keys not supported.");
	}

	@Override
	public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException(
			"Record deserialization with leading normalized keys not supported.");
	}

	@Override
	public boolean invertNormalizedKey() {
		return wrappedComparator.invertNormalizedKey();
	}

	@Override
	public TypeComparator<T> duplicate() {
		return new NullAwareComparator<T>(wrappedComparator.duplicate(), order);
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		if (record == null) {
			for (int i = 0; i < flatFields; i++) {
				target[index + i] = null;
			}
			return flatFields;
		} else {
			return wrappedComparator.extractKeys(record, target, index);
		}
	}

	@Override
	public TypeComparator[] getFlatComparators() {
		// determine the flat comparators and wrap them again in null-aware comparators
		List<TypeComparator<?>> flatComparators = new ArrayList<>();
		if (wrappedComparator instanceof CompositeTypeComparator) {
			((CompositeTypeComparator) wrappedComparator).getFlatComparator(flatComparators);
		} else {
			flatComparators.add(wrappedComparator);
		}

		TypeComparator<?>[] result = new TypeComparator[flatComparators.size()];
		for (int i = 0; i < result.length; i++) {
			result[i] = new NullAwareComparator<>(flatComparators.get(i), order);
		}
		return result;
	}
}
