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

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BaseArray;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryArrayWriter;
import org.apache.flink.table.dataformat.GenericArray;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.List;

/**
 * Serializer for BaseArray.
 */
public class BaseArraySerializer extends TypeSerializer<BaseArray> {

	private final boolean isPrimitive;
	private final InternalType eleType;
	private final Class<?> internalEleClass;

	private TypeSerializer elementSerializer;
	private BinaryArraySerializer binarySerializer;

	private BinaryArray reuseBinaryArray;
	private BinaryArrayWriter reuseBinaryWriter;

	public BaseArraySerializer(boolean isPrimitive, InternalType eleType) {
		this.isPrimitive = isPrimitive;
		this.eleType = eleType;
		if (isPrimitive) {
			this.internalEleClass = TypeUtils.getPrimitiveInternalClassForType(eleType);
		} else {
			this.internalEleClass = TypeUtils.getInternalClassForType(eleType);
		}

		this.elementSerializer = DataTypes.createInternalSerializer(eleType);
		this.binarySerializer = BinaryArraySerializer.INSTANCE;
	}

	public boolean isPrimitive() {
		return isPrimitive;
	}

	public InternalType getElementType() {
		return eleType;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<BaseArray> duplicate() {
		return new BaseArraySerializer(isPrimitive, eleType);
	}

	@Override
	public BaseArray createInstance() {
		return new GenericArray(0, isPrimitive, internalEleClass);
	}

	@Override
	public BaseArray copy(BaseArray from) {
		return copy(from, new GenericArray(from.numElements(), isPrimitive, internalEleClass));
	}

	@Override
	public BaseArray copy(BaseArray from, BaseArray reuse) {
		int numElements = from.numElements();
		GenericArray ret;
		if (reuse instanceof GenericArray) {
			ret = (GenericArray) reuse;
		} else {
			ret = new GenericArray(numElements, isPrimitive, internalEleClass);
		}

		for (int i = 0; i < numElements; i++) {
			if (from.isNullAt(i)) {
				ret.setNullAt(i);
			} else {
				ret.setNotNullAt(i);
				if (isPrimitive) {
					ret.setPrimitive(i, TypeGetterSetters.get(from, i, eleType), eleType);
				} else {
					Object element = TypeGetterSetters.get(from, i, eleType);
					if (from instanceof GenericArray) {
						element = elementSerializer.copy(element);
					}
					ret.setObject(i, element);
				}
			}
		}

		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BaseArray record, DataOutputView target) throws IOException {
		BinaryArray binaryArray = baseArrayToBinary(record);
		target.write(binaryArray.getBytes());
	}

	public BinaryArray baseArrayToBinary(BaseArray from) {
		if (from instanceof BinaryArray) {
			return (BinaryArray) from;
		}

		int numElements = from.numElements();
		if (reuseBinaryArray == null) {
			reuseBinaryArray = new BinaryArray();
		}
		if (reuseBinaryWriter == null || reuseBinaryWriter.getNumElements() != numElements) {
			reuseBinaryWriter = new BinaryArrayWriter(
				reuseBinaryArray, numElements, BinaryArray.calculateElementSize(eleType));
		} else {
			reuseBinaryWriter.reset();
		}

		for (int i = 0; i < numElements; i++) {
			if (from.isNullAt(i)) {
				reuseBinaryWriter.setNullAt(i, eleType);
			} else {
				BaseRowUtil.write(reuseBinaryWriter, i,
						TypeGetterSetters.get(from, i, eleType), eleType, elementSerializer);
			}
		}
		reuseBinaryWriter.complete();

		return reuseBinaryArray;
	}

	@Override
	public BaseArray deserialize(DataInputView source) throws IOException {
		return binarySerializer.deserialize(source);
	}

	@Override
	public BaseArray deserialize(BaseArray reuse, DataInputView source) throws IOException {
		if (reuse instanceof BinaryArray) {
			return binarySerializer.deserialize((BinaryArray) reuse, source);
		} else {
			return binarySerializer.deserialize(source);
		}
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	@Override
	public boolean equals(Object obj) {
		if (canEqual(obj)) {
			BaseArraySerializer other = (BaseArraySerializer) obj;
			return isPrimitive == other.isPrimitive()
				&& eleType.equals(other.getElementType());
		}

		return false;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BaseArraySerializer;
	}

	@Override
	public int hashCode() {
		return 31 * eleType.hashCode() + (isPrimitive ? 1 : 0);
	}

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		return new ArraySerializerConfigSnapshot(
			isPrimitive, eleType, elementSerializer, binarySerializer);
	}

	@Override
	public CompatibilityResult<BaseArray> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (!(configSnapshot instanceof ArraySerializerConfigSnapshot)) {
			return CompatibilityResult.requiresMigration();
		}

		ArraySerializerConfigSnapshot config = (ArraySerializerConfigSnapshot) configSnapshot;
		if (config.isPrimitive() != isPrimitive || !config.getElementType().equals(eleType)) {
			return CompatibilityResult.requiresMigration();
		}

		TypeSerializer[] serializers = {elementSerializer, binarySerializer};
		List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> previousFieldSerializersAndConfigs =
			((AbstractRowSerializer.RowSerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();
		if (previousFieldSerializersAndConfigs.size() != serializers.length) {
			return CompatibilityResult.requiresMigration();
		}

		for (int i = 0; i < serializers.length; i++) {
			CompatibilityResult<BinaryArray> compatResult = CompatibilityUtil.resolveCompatibilityResult(
				previousFieldSerializersAndConfigs.get(i).f0,
				UnloadableDummyTypeSerializer.class,
				previousFieldSerializersAndConfigs.get(i).f1,
				serializers[i]);
			if (compatResult.isRequiresMigration()) {
				return CompatibilityResult.requiresMigration();
			}
		}

		return CompatibilityResult.compatible();
	}

	/**
	 * Snapshot.
	 */
	public static final class ArraySerializerConfigSnapshot extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		private boolean isPrimitive;
		private InternalType eleType;

		public ArraySerializerConfigSnapshot(
			boolean isPrimitive, InternalType eleType,
			TypeSerializer elementSerializer,
			BinaryArraySerializer binarySerializer) {
			super(elementSerializer, binarySerializer);

			this.isPrimitive = isPrimitive;
			this.eleType = eleType;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);

			try (final DataOutputViewStream outViewWrapper = new DataOutputViewStream(out)) {
				InstantiationUtil.serializeObject(outViewWrapper, isPrimitive);
				InstantiationUtil.serializeObject(outViewWrapper, eleType);
			}
		}

		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);

			try (final DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
				isPrimitive = InstantiationUtil.deserializeObject(inViewWrapper, getUserCodeClassLoader());
				eleType = InstantiationUtil.deserializeObject(inViewWrapper, getUserCodeClassLoader());
			} catch (ClassNotFoundException e) {
				throw new IOException("Could not find requested element class in classpath.", e);
			}
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		public boolean isPrimitive() {
			return isPrimitive;
		}

		public InternalType getElementType() {
			return eleType;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ArraySerializerConfigSnapshot) {
				ArraySerializerConfigSnapshot other = (ArraySerializerConfigSnapshot) obj;
				return super.equals(obj)
					&& isPrimitive == other.isPrimitive()
					&& eleType.equals(other.getElementType());
			}

			return false;
		}

		@Override
		public int hashCode() {
			return 31 * (31 * super.hashCode() + eleType.hashCode()) + (isPrimitive ? 1 : 0);
		}
	}
}
