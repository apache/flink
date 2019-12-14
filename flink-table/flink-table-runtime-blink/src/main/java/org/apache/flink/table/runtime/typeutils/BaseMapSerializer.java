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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.dataformat.BaseMap;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryArrayWriter;
import org.apache.flink.table.dataformat.BinaryMap;
import org.apache.flink.table.dataformat.BinaryWriter;
import org.apache.flink.table.dataformat.GenericMap;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.runtime.util.SegmentsUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializer for {@link BaseMap}.
 */
public class BaseMapSerializer extends TypeSerializer<BaseMap> {

	private static final Logger LOG = LoggerFactory.getLogger(BaseMapSerializer.class);

	private final LogicalType keyType;
	private final LogicalType valueType;

	private final TypeSerializer keySerializer;
	private final TypeSerializer valueSerializer;

	private transient BinaryArray reuseKeyArray;
	private transient BinaryArray reuseValueArray;
	private transient BinaryArrayWriter reuseKeyWriter;
	private transient BinaryArrayWriter reuseValueWriter;

	public BaseMapSerializer(LogicalType keyType, LogicalType valueType, ExecutionConfig conf) {
		this.keyType = keyType;
		this.valueType = valueType;

		this.keySerializer = InternalSerializers.create(keyType, conf);
		this.valueSerializer = InternalSerializers.create(valueType, conf);
	}

	private BaseMapSerializer(
			LogicalType keyType, LogicalType valueType, TypeSerializer keySerializer, TypeSerializer valueSerializer) {
		this.keyType = keyType;
		this.valueType = valueType;

		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<BaseMap> duplicate() {
		return new BaseMapSerializer(keyType, valueType, keySerializer.duplicate(), valueSerializer.duplicate());
	}

	@Override
	public BaseMap createInstance() {
		return new BinaryMap();
	}

	/**
	 * NOTE: Map should be a HashMap, when we insert the key/value pairs of the TreeMap into
	 * a HashMap, problems maybe occur.
	 */
	@Override
	public BaseMap copy(BaseMap from) {
		if (from instanceof GenericMap) {
			Map<Object, Object> fromMap = ((GenericMap) from).getMap();
			if (!(fromMap instanceof HashMap) && LOG.isDebugEnabled()) {
				LOG.debug("It is dangerous to copy a non-HashMap to a HashMap.");
			}
			HashMap<Object, Object> toMap = new HashMap<>();
			for (Map.Entry<Object, Object> entry : fromMap.entrySet()) {
				toMap.put(
						keySerializer.copy(entry.getKey()),
						valueSerializer.copy(entry.getValue()));
			}
			return new GenericMap(toMap);
		} else {
			return ((BinaryMap) from).copy();
		}
	}

	@Override
	public BaseMap copy(BaseMap from, BaseMap reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BaseMap record, DataOutputView target) throws IOException {
		BinaryMap binaryMap = toBinaryMap(record);
		target.writeInt(binaryMap.getSizeInBytes());
		SegmentsUtil.copyToView(binaryMap.getSegments(), binaryMap.getOffset(), binaryMap.getSizeInBytes(), target);
	}

	public BinaryMap toBinaryMap(BaseMap from) {
		if (from instanceof BinaryMap) {
			return (BinaryMap) from;
		}

		Map<Object, Object> javaMap = ((GenericMap) from).getMap();
		int numElements = javaMap.size();
		if (reuseKeyArray == null) {
			reuseKeyArray = new BinaryArray();
		}
		if (reuseValueArray == null) {
			reuseValueArray = new BinaryArray();
		}
		if (reuseKeyWriter == null || reuseKeyWriter.getNumElements() != numElements) {
			reuseKeyWriter = new BinaryArrayWriter(
					reuseKeyArray, numElements, BinaryArray.calculateFixLengthPartSize(keyType));
		} else {
			reuseKeyWriter.reset();
		}
		if (reuseValueWriter == null || reuseValueWriter.getNumElements() != numElements) {
			reuseValueWriter = new BinaryArrayWriter(
					reuseValueArray, numElements, BinaryArray.calculateFixLengthPartSize(valueType));
		} else {
			reuseValueWriter.reset();
		}

		int i = 0;
		for (Map.Entry<Object, Object> entry : javaMap.entrySet()) {
			if (entry.getKey() == null) {
				reuseKeyWriter.setNullAt(i, keyType);
			} else {
				BinaryWriter.write(reuseKeyWriter, i, entry.getKey(), keyType, keySerializer);
			}
			if (entry.getValue() == null) {
				reuseValueWriter.setNullAt(i, valueType);
			} else {
				BinaryWriter.write(reuseValueWriter, i, entry.getValue(), valueType, valueSerializer);
			}
			i++;
		}
		reuseKeyWriter.complete();
		reuseValueWriter.complete();

		return BinaryMap.valueOf(reuseKeyArray, reuseValueArray);
	}

	@Override
	public BaseMap deserialize(DataInputView source) throws IOException {
		return deserializeReuse(new BinaryMap(), source);
	}

	@Override
	public BaseMap deserialize(BaseMap reuse, DataInputView source) throws IOException {
		return deserializeReuse(reuse instanceof GenericMap ? new BinaryMap() : (BinaryMap) reuse, source);
	}

	private BinaryMap deserializeReuse(BinaryMap reuse, DataInputView source) throws IOException {
		int length = source.readInt();
		byte[] bytes = new byte[length];
		source.readFully(bytes);
		reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		BaseMapSerializer that = (BaseMapSerializer) o;

		return keyType.equals(that.keyType) && valueType.equals(that.valueType);
	}

	@Override
	public int hashCode() {
		int result = keyType.hashCode();
		result = 31 * result + valueType.hashCode();
		return result;
	}

	@VisibleForTesting
	public TypeSerializer getKeySerializer() {
		return keySerializer;
	}

	@VisibleForTesting
	public TypeSerializer getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public TypeSerializerSnapshot<BaseMap> snapshotConfiguration() {
		return new BaseMapSerializerSnapshot(keyType, valueType, keySerializer, valueSerializer);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BaseArraySerializer}.
	 */
	public static final class BaseMapSerializerSnapshot implements TypeSerializerSnapshot<BaseMap> {
		private static final int CURRENT_VERSION = 3;

		private LogicalType previousKeyType;
		private LogicalType previousValueType;

		private TypeSerializer previousKeySerializer;
		private TypeSerializer previousValueSerializer;

		@SuppressWarnings("unused")
		public BaseMapSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		BaseMapSerializerSnapshot(LogicalType keyT, LogicalType valueT, TypeSerializer keySer, TypeSerializer valueSer) {
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
			return new BaseMapSerializer(
				previousKeyType, previousValueType, previousKeySerializer, previousValueSerializer);
		}

		@Override
		public TypeSerializerSchemaCompatibility<BaseMap> resolveSchemaCompatibility(TypeSerializer<BaseMap> newSerializer) {
			if (!(newSerializer instanceof BaseMapSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			BaseMapSerializer newBaseMapSerializer = (BaseMapSerializer) newSerializer;
			if (!previousKeyType.equals(newBaseMapSerializer.keyType) ||
				!previousValueType.equals(newBaseMapSerializer.valueType) ||
				!previousKeySerializer.equals(newBaseMapSerializer.keySerializer) ||
				!previousValueSerializer.equals(newBaseMapSerializer.valueSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
