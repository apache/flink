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

import org.apache.flink.api.common.typeutils.CompositeTypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.util.List;

public class GenTypeComparatorProxy<T> extends CompositeTypeComparator<T> implements java.io.Serializable {
	private final String code;
	private final String name;
	private final Class<T> clazz;
	private final TypeComparator<?>[] comparators;
	private final TypeSerializer<T> serializer;

	transient private CompositeTypeComparator<T> impl = null;

	private void compile() {
		try {
			assert impl == null;
			Class<?> comparatorClazz = InstantiationUtil.compile(clazz.getClassLoader(), name, code);
			Constructor<?>[] ctors = comparatorClazz.getConstructors();
			assert ctors.length == 1;
			impl = (CompositeTypeComparator<T>) ctors[0].newInstance(comparators, serializer, clazz);
		} catch (Exception e) {
			throw new RuntimeException("Unable to generate serializer: " + name, e);
		}
	}

	public GenTypeComparatorProxy(Class<T> clazz, String name, String code,TypeComparator<?>[] comparators,
									TypeSerializer<T> serializer) {
		this.name = name;
		this.code = code;
		this.clazz = clazz;
		this.comparators = comparators;
		this.serializer = serializer;
		compile();
	}

	private GenTypeComparatorProxy(GenTypeComparatorProxy<T> other) {
		this.name = other.name;
		this.code = other.code;
		this.clazz = other.clazz;
		this.comparators = new TypeComparator[other.comparators.length];
		for (int i = 0; i < other.comparators.length; i++) {
			this.comparators[i] = other.comparators[i].duplicate();
		}
		this.serializer = other.serializer.duplicate();
		if (other.impl != null) {
			this.impl = (CompositeTypeComparator<T>) other.impl.duplicate();
		}
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		compile();
	}

	@Override
	public void getFlatComparator(List<TypeComparator> flatComparators) {
		impl.getFlatComparator(flatComparators);
	}

	@Override
	public int hash(T value) {
		return impl.hash(value);
	}

	@Override
	public void setReference(T toCompare) {
		impl.setReference(toCompare);
	}

	@Override
	public boolean equalToReference(T candidate) {
		return impl.equalToReference(candidate);
	}

	@Override
	public int compareToReference(TypeComparator<T> referencedComparator) {
		return impl.compareToReference(referencedComparator);
	}

	@Override
	public int compare(T first, T second) {
		return impl.compare(first, second);
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		return impl.compareSerialized(firstSource, secondSource);
	}

	@Override
	public boolean supportsNormalizedKey() {
		return impl.supportsNormalizedKey();
	}

	@Override
	public int getNormalizeKeyLen() {
		return impl.getNormalizeKeyLen();
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return impl.isNormalizedKeyPrefixOnly(keyBytes);
	}

	@Override
	public void putNormalizedKey(T value, MemorySegment target, int offset, int numBytes) {
		impl.putNormalizedKey(value, target, offset, numBytes);
	}

	@Override
	public boolean invertNormalizedKey() {
		return impl.invertNormalizedKey();
	}


	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return impl.supportsSerializationWithKeyNormalization();
	}

	@Override
	public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
		impl.writeWithKeyNormalization(record, target);
	}

	@Override
	public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
		return impl.readWithKeyDenormalization(reuse, source);
	}

	@Override
	public GenTypeComparatorProxy<T> duplicate() {
		return new GenTypeComparatorProxy<>(this);
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		return impl.extractKeys(record, target, index);
	}

	public TypeComparator<T> getImpl() {
		return impl;
	}
}
