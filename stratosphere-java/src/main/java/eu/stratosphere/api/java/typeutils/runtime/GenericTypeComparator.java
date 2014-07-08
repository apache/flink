/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.typeutils.runtime;

import com.esotericsoftware.kryo.Kryo;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.types.NormalizableKey;
import eu.stratosphere.util.InstantiationUtil;

import java.io.IOException;

/**
 * TypeComparator for all types that extend Comparable.
 */
public class GenericTypeComparator<T extends Comparable<T>> extends TypeComparator<T> {

	private static final long serialVersionUID = 1L;

	private final boolean ascending;

	private final Class<T> type;

	private final TypeSerializerFactory<T> serializerFactory;

	private transient TypeSerializer<T> serializer;

	private transient T reference;

	private transient T tmpReference;

	private transient Kryo kryo;

	// ------------------------------------------------------------------------

	public GenericTypeComparator(boolean ascending, TypeSerializer<T> serializer, Class<T> type) {
		this.ascending = ascending;
		this.serializer = serializer;
		this.type = type;

		this.serializerFactory = this.serializer.isStateful()
				? new RuntimeStatefulSerializerFactory<T>(this.serializer, this.type)
				: new RuntimeStatelessSerializerFactory<T>(this.serializer, this.type);
	}

	private GenericTypeComparator(GenericTypeComparator<T> toClone) {
		this.ascending = toClone.ascending;
		this.serializerFactory = toClone.serializerFactory;
		this.type = toClone.type;
	}

	@Override
	public int hash(T record) {
		return record.hashCode();
	}

	@Override
	public void setReference(T toCompare) {
		checkKryoInitialized();
		this.reference = this.kryo.copy(toCompare);
	}

	@Override
	public boolean equalToReference(T candidate) {
		return candidate.equals(this.reference);
	}

	@Override
	public int compareToReference(TypeComparator<T> referencedComparator) {
		T otherRef = ((GenericTypeComparator<T>) referencedComparator).reference;
		int cmp = otherRef.compareTo(this.reference);

		return this.ascending ? cmp : -cmp;
	}

	@Override
	public int compare(T first, T second) {
		return first.compareTo(second);
	}

	@Override
	public int compare(final DataInputView firstSource, final DataInputView secondSource) throws IOException {
		if (this.serializer == null) {
			this.serializer = this.serializerFactory.getSerializer();
		}

		if (this.reference == null) {
			this.reference = this.serializer.createInstance();
		}

		if (this.tmpReference == null) {
			this.tmpReference = this.serializer.createInstance();
		}

		this.reference = this.serializer.deserialize(this.reference, firstSource);
		this.tmpReference = this.serializer.deserialize(this.tmpReference, secondSource);

		int cmp = this.reference.compareTo(this.tmpReference);
		return this.ascending ? cmp : -cmp;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return NormalizableKey.class.isAssignableFrom(this.type);
	}

	@Override
	public int getNormalizeKeyLen() {
		if (this.reference == null) {
			this.reference = InstantiationUtil.instantiate(this.type);
		}

		NormalizableKey<?> key = (NormalizableKey<?>) this.reference;
		return key.getMaxNormalizedKeyLen();
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < getNormalizeKeyLen();
	}

	@Override
	public void putNormalizedKey(T record, MemorySegment target, int offset, int numBytes) {
		NormalizableKey<?> key = (NormalizableKey<?>) record;
		key.copyNormalizedKey(target, offset, numBytes);
	}

	@Override
	public boolean invertNormalizedKey() {
		return !ascending;
	}

	@Override
	public TypeComparator<T> duplicate() {
		return new GenericTypeComparator<T>(this);
	}

	private final void checkKryoInitialized() {
		if (this.kryo == null) {
			this.kryo = new Kryo();
			this.kryo.setAsmEnabled(true);
			this.kryo.register(this.type);
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}
}
