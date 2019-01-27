/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BaseMap;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryArrayWriter;
import org.apache.flink.table.dataformat.BinaryMap;
import org.apache.flink.table.dataformat.GenericMap;
import org.apache.flink.table.dataformat.util.BaseRowUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializer for BaseMap.
 */
public class BaseMapSerializer extends TypeSerializer<BaseMap> {

	private final InternalType keyType;
	private final InternalType valueType;

	private final TypeSerializer keySerializer;
	private final TypeSerializer valueSerializer;
	private final BinaryMapSerializer binarySerializer;

	private BinaryArray reuseKeyArray;
	private BinaryArray reuseValueArray;
	private BinaryArrayWriter reuseKeyWriter;
	private BinaryArrayWriter reuseValueWriter;

	public BaseMapSerializer(InternalType keyType, InternalType valueType) {
		this.keyType = keyType;
		this.valueType = valueType;

		this.keySerializer = DataTypes.createInternalSerializer(keyType);
		this.valueSerializer = DataTypes.createInternalSerializer(valueType);
		this.binarySerializer = BinaryMapSerializer.INSTANCE;
	}

	public InternalType getKeyType() {
		return keyType;
	}

	public InternalType getValueType() {
		return valueType;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<BaseMap> duplicate() {
		return null;
	}

	@Override
	public BaseMap createInstance() {
		return new GenericMap();
	}

	@Override
	public BaseMap copy(BaseMap from) {
		return copy(from, null);
	}

	@Override
	public BaseMap copy(BaseMap from, BaseMap reuse) {
		if (from instanceof GenericMap) {
			Map<Object, Object> fromMap = from.toJavaMap(keyType, valueType);
			HashMap<Object, Object> toMap = new HashMap<>();
			for (Map.Entry<Object, Object> entry : fromMap.entrySet()) {
				toMap.put(
					keySerializer.copy(entry.getKey()),
					valueSerializer.copy(entry.getValue()));
			}
			return new GenericMap(toMap);
		} else {
			// `from` is a binary map, and the contents are read from memory segments,
			// so no need to copy
			return new GenericMap(from.toJavaMap(keyType, valueType));
		}
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BaseMap record, DataOutputView target) throws IOException {
		BinaryMap binaryMap = baseMapToBinary(record);
		target.write(binaryMap.getBytes());
	}

	public BinaryMap baseMapToBinary(BaseMap from) {
		if (from instanceof BinaryMap) {
			return (BinaryMap) from;
		}

		Map<Object, Object> javaMap = from.toJavaMap(keyType, valueType);
		int numElements = javaMap.size();
		if (reuseKeyArray == null) {
			reuseKeyArray = new BinaryArray();
		}
		if (reuseValueArray == null) {
			reuseValueArray = new BinaryArray();
		}
		if (reuseKeyWriter == null || reuseKeyWriter.getNumElements() != numElements) {
			reuseKeyWriter = new BinaryArrayWriter(
				reuseKeyArray, numElements, BinaryArray.calculateElementSize(keyType));
		} else {
			reuseKeyWriter.reset();
		}
		if (reuseValueWriter == null || reuseValueWriter.getNumElements() != numElements) {
			reuseValueWriter = new BinaryArrayWriter(
				reuseValueArray, numElements, BinaryArray.calculateElementSize(valueType));
		} else {
			reuseValueWriter.reset();
		}

		int i = 0;
		for (Map.Entry<Object, Object> entry : javaMap.entrySet()) {
			if (entry.getKey() == null) {
				reuseKeyWriter.setNullAt(i, keyType);
			} else {
				BaseRowUtil.write(reuseKeyWriter, i, entry.getKey(), keyType, keySerializer);
			}
			if (entry.getValue() == null) {
				reuseValueWriter.setNullAt(i, valueType);
			} else {
				BaseRowUtil.write(reuseValueWriter, i, entry.getValue(), valueType, valueSerializer);
			}
			i++;
		}
		reuseKeyWriter.complete();
		reuseValueWriter.complete();

		return BinaryMap.valueOf(reuseKeyArray, reuseValueArray);
	}

	@Override
	public BaseMap deserialize(DataInputView source) throws IOException {
		return binarySerializer.deserialize(source);
	}

	@Override
	public BaseMap deserialize(BaseMap reuse, DataInputView source) throws IOException {
		if (reuse instanceof BinaryMap) {
			return binarySerializer.deserialize((BinaryMap) reuse, source);
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
			BaseMapSerializer other = (BaseMapSerializer) obj;
			return keyType.equals(other.getKeyType())
				&& valueType.equals(other.getValueType());
		}

		return false;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BaseMapSerializer;
	}

	@Override
	public int hashCode() {
		return 31 * keyType.hashCode() + valueType.hashCode();
	}

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		return new CompositeTypeSerializerConfigSnapshot(
			keySerializer, valueSerializer, binarySerializer) {

			private static final int VERSION = 1;

			@Override
			public int getVersion() {
				return VERSION;
			}
		};
	}

	@Override
	public CompatibilityResult<BaseMap> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (!(configSnapshot instanceof BaseArraySerializer.ArraySerializerConfigSnapshot)) {
			return CompatibilityResult.requiresMigration();
		}

		TypeSerializer[] serializers = {keySerializer, valueSerializer, binarySerializer};
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
}
