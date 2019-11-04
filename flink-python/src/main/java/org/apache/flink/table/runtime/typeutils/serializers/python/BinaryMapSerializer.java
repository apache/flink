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

package org.apache.flink.table.runtime.typeutils.serializers.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.dataformat.BaseMap;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryArrayWriter;
import org.apache.flink.table.dataformat.BinaryMap;
import org.apache.flink.table.dataformat.BinaryWriter;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.runtime.typeutils.BaseMapSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * A {@link TypeSerializer} for {@link BinaryMap}. It should be noted that the header will not be encoded.
 * Currently Python doesn't support BinaryMap natively, so we can't use BaseArraySerializer in blink directly.
 */
@Internal
public class BinaryMapSerializer<K, V> extends BaseMapSerializer {

	private static final long serialVersionUID = 1L;

	private final LogicalType keyType;

	private final LogicalType valueType;

	private final TypeSerializer<K> keyTypeSerializer;

	private final TypeSerializer<V> valueTypeSerializer;

	private final int keySize;

	private final int valueSize;

	public BinaryMapSerializer(LogicalType keyType, LogicalType valueType
		, TypeSerializer<K> keyTypeSerializer, TypeSerializer<V> valueTypeSerializer) {
		super(keyType, valueType, null);
		this.keyType = keyType;
		this.valueType = valueType;
		this.keyTypeSerializer = keyTypeSerializer;
		this.valueTypeSerializer = valueTypeSerializer;
		this.keySize = BinaryArray.calculateFixLengthPartSize(this.keyType);
		this.valueSize = BinaryArray.calculateFixLengthPartSize(this.valueType);
	}

	@Override
	public void serialize(BaseMap map, DataOutputView target) throws IOException {
		BinaryMap binaryMap = (BinaryMap) map;
		final int size = binaryMap.numElements();
		target.writeInt(size);
		BinaryArray keyArray = binaryMap.keyArray();
		BinaryArray valueArray = binaryMap.valueArray();
		for (int i = 0; i < size; i++) {
			if (keyArray.isNullAt(i)) {
				throw new IllegalArgumentException("The key of BinaryMap must not be null.");
			}
			K key = (K) TypeGetterSetters.get(keyArray, i, keyType);
			keyTypeSerializer.serialize(key, target);
			if (valueArray.isNullAt(i)) {
				target.writeBoolean(true);
			} else {
				target.writeBoolean(false);
				V value = (V) TypeGetterSetters.get(valueArray, i, valueType);
				valueTypeSerializer.serialize(value, target);
			}
		}
	}

	@Override
	public BaseMap deserialize(DataInputView source) throws IOException {
		final int size = source.readInt();
		BinaryArray keyArray = new BinaryArray();
		BinaryArray valueArray = new BinaryArray();
		BinaryArrayWriter keyWriter = new BinaryArrayWriter(keyArray, size, keySize);
		BinaryArrayWriter valueWriter = new BinaryArrayWriter(valueArray, size, valueSize);
		for (int i = 0; i < size; i++) {
			K key = keyTypeSerializer.deserialize(source);
			BinaryWriter.write(keyWriter, i, key, keyType, keyTypeSerializer);
			boolean isNull = source.readBoolean();
			if (isNull) {
				valueWriter.setNullAt(i);
			} else {
				V value = valueTypeSerializer.deserialize(source);
				BinaryWriter.write(valueWriter, i, value, valueType, valueTypeSerializer);
			}
		}
		keyWriter.complete();
		valueWriter.complete();
		return BinaryMap.valueOf(keyArray, valueArray);
	}

	@Override
	public BaseMap deserialize(BaseMap reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		serialize(deserialize(source), target);
	}

	@Override
	public TypeSerializer<BaseMap> duplicate() {
		return new BinaryMapSerializer<>(keyType, valueType, keyTypeSerializer, valueTypeSerializer);
	}

	@Override
	public TypeSerializerSnapshot<BaseMap> snapshotConfiguration() {
		return new BinaryMapSerializerSnapshot(keyType, valueType, keyTypeSerializer, valueTypeSerializer);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BinaryMapSerializer}.
	 */
	public static final class BinaryMapSerializerSnapshot implements TypeSerializerSnapshot<BaseMap> {
		private static final int CURRENT_VERSION = 3;

		private LogicalType previousKeyType;
		private LogicalType previousValueType;

		private TypeSerializer previousKeySerializer;
		private TypeSerializer previousValueSerializer;

		@SuppressWarnings("unused")
		public BinaryMapSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		BinaryMapSerializerSnapshot(LogicalType keyT, LogicalType valueT, TypeSerializer keySer, TypeSerializer valueSer) {
			this.previousKeyType = keyT;
			this.previousValueType = valueT;

			this.previousKeySerializer = keySer;
			this.previousValueSerializer = valueSer;
		}

		@Override
		public int getCurrentVersion() {
			return CURRENT_VERSION;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			DataOutputViewStream outStream = new DataOutputViewStream(out);
			InstantiationUtil.serializeObject(outStream, previousKeyType);
			InstantiationUtil.serializeObject(outStream, previousValueType);
			InstantiationUtil.serializeObject(outStream, previousKeySerializer);
			InstantiationUtil.serializeObject(outStream, previousValueSerializer);
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
			try {
				DataInputViewStream inStream = new DataInputViewStream(in);
				this.previousKeyType = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
				this.previousValueType = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
				this.previousKeySerializer = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
				this.previousValueSerializer = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		@Override
		public TypeSerializer<BaseMap> restoreSerializer() {
			return new BinaryMapSerializer(
				previousKeyType, previousValueType, previousKeySerializer, previousValueSerializer);
		}

		@Override
		public TypeSerializerSchemaCompatibility<BaseMap> resolveSchemaCompatibility(TypeSerializer<BaseMap> newSerializer) {
			if (!(newSerializer instanceof BinaryMapSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			BinaryMapSerializer newBaseMapSerializer = (BinaryMapSerializer) newSerializer;
			if (!previousKeyType.equals(newBaseMapSerializer.keyType) ||
				!previousValueType.equals(newBaseMapSerializer.valueType) ||
				!previousKeySerializer.equals(newBaseMapSerializer.keyTypeSerializer) ||
				!previousValueSerializer.equals(newBaseMapSerializer.valueTypeSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
