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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.ColumnarArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Serializer for {@link ArrayData}.
 */
@Internal
public class ArrayDataSerializer extends TypeSerializer<ArrayData> {
	private static final long serialVersionUID = 1L;

	private final LogicalType eleType;
	private final TypeSerializer<Object> eleSer;
	private final ArrayData.ElementGetter elementGetter;

	private transient BinaryArrayData reuseArray;
	private transient BinaryArrayWriter reuseWriter;

	public ArrayDataSerializer(LogicalType eleType) {
		this(eleType, InternalSerializers.create(eleType));
	}

	private ArrayDataSerializer(LogicalType eleType, TypeSerializer<Object> eleSer) {
		this.eleType = eleType;
		this.eleSer = eleSer;
		this.elementGetter = ArrayData.createElementGetter(eleType);
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<ArrayData> duplicate() {
		return new ArrayDataSerializer(eleType, eleSer.duplicate());
	}

	@Override
	public ArrayData createInstance() {
		return new BinaryArrayData();
	}

	@Override
	public ArrayData copy(ArrayData from) {
		if (from instanceof GenericArrayData) {
			return copyGenericArray((GenericArrayData) from);
		} else if (from instanceof BinaryArrayData) {
			return ((BinaryArrayData) from).copy();
		} else {
			return copyColumnarArray((ColumnarArrayData) from);
		}
	}

	@Override
	public ArrayData copy(ArrayData from, ArrayData reuse) {
		return copy(from);
	}

	private GenericArrayData copyGenericArray(GenericArrayData array) {
		if (array.isPrimitiveArray()) {
			switch (eleType.getTypeRoot()) {
				case BOOLEAN:
					return new GenericArrayData(Arrays.copyOf(array.toBooleanArray(), array.size()));
				case TINYINT:
					return new GenericArrayData(Arrays.copyOf(array.toByteArray(), array.size()));
				case SMALLINT:
					return new GenericArrayData(Arrays.copyOf(array.toShortArray(), array.size()));
				case INTEGER:
					return new GenericArrayData(Arrays.copyOf(array.toIntArray(), array.size()));
				case BIGINT:
					return new GenericArrayData(Arrays.copyOf(array.toLongArray(), array.size()));
				case FLOAT:
					return new GenericArrayData(Arrays.copyOf(array.toFloatArray(), array.size()));
				case DOUBLE:
					return new GenericArrayData(Arrays.copyOf(array.toDoubleArray(), array.size()));
				default:
					throw new RuntimeException("Unknown type: " + eleType);
			}
		} else {
			Object[] objectArray = array.toObjectArray();
			Object[] newArray = (Object[]) Array.newInstance(
				LogicalTypeUtils.toInternalConversionClass(eleType),
				array.size());
			for (int i = 0; i < array.size(); i++) {
				newArray[i] = eleSer.copy(objectArray[i]);
			}
			return new GenericArrayData(newArray);
		}
	}

	private GenericArrayData copyColumnarArray(ColumnarArrayData from) {
		if (!eleType.isNullable()) {
			// we don't need to copy the primitive arrays,
			// because they are new array from ColumnarArrayData
			switch (eleType.getTypeRoot()) {
				case BOOLEAN:
					return new GenericArrayData(from.toBooleanArray());
				case TINYINT:
					return new GenericArrayData(from.toByteArray());
				case SMALLINT:
					return new GenericArrayData(from.toShortArray());
				case INTEGER:
				case DATE:
				case TIME_WITHOUT_TIME_ZONE:
					return new GenericArrayData(from.toIntArray());
				case BIGINT:
					return new GenericArrayData(from.toLongArray());
				case FLOAT:
					return new GenericArrayData(from.toFloatArray());
				case DOUBLE:
					return new GenericArrayData(from.toDoubleArray());
			}
		}

		Object[] newArray = new Object[from.size()];
		for (int i = 0; i < newArray.length; i++) {
			if (!from.isNullAt(i)) {
				newArray[i] = eleSer.copy(elementGetter.getElementOrNull(from, i));
			} else {
				newArray[i] = null;
			}
		}
		return new GenericArrayData(newArray);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(ArrayData record, DataOutputView target) throws IOException {
		BinaryArrayData binaryArray = toBinaryArray(record);
		target.writeInt(binaryArray.getSizeInBytes());
		BinarySegmentUtils.copyToView(binaryArray.getSegments(), binaryArray.getOffset(), binaryArray.getSizeInBytes(), target);
	}

	public BinaryArrayData toBinaryArray(ArrayData from) {
		if (from instanceof BinaryArrayData) {
			return (BinaryArrayData) from;
		}

		int numElements = from.size();
		if (reuseArray == null) {
			reuseArray = new BinaryArrayData();
		}
		if (reuseWriter == null || reuseWriter.getNumElements() != numElements) {
			reuseWriter = new BinaryArrayWriter(
				reuseArray, numElements, BinaryArrayData.calculateFixLengthPartSize(eleType));
		} else {
			reuseWriter.reset();
		}

		for (int i = 0; i < numElements; i++) {
			if (from.isNullAt(i)) {
				reuseWriter.setNullAt(i, eleType);
			} else {
				BinaryWriter.write(reuseWriter, i, elementGetter.getElementOrNull(from, i), eleType, eleSer);
			}
		}
		reuseWriter.complete();

		return reuseArray;
	}

	@Override
	public ArrayData deserialize(DataInputView source) throws IOException {
		return deserializeReuse(new BinaryArrayData(), source);
	}

	@Override
	public ArrayData deserialize(ArrayData reuse, DataInputView source) throws IOException {
		return deserializeReuse(
			reuse instanceof BinaryArrayData ? (BinaryArrayData) reuse : new BinaryArrayData(),
			source);
	}

	private BinaryArrayData deserializeReuse(BinaryArrayData reuse, DataInputView source) throws IOException {
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

		ArrayDataSerializer that = (ArrayDataSerializer) o;

		return eleType.equals(that.eleType);
	}

	@Override
	public int hashCode() {
		return eleType.hashCode();
	}

	@VisibleForTesting
	public TypeSerializer getEleSer() {
		return eleSer;
	}

	@Override
	public TypeSerializerSnapshot<ArrayData> snapshotConfiguration() {
		return new ArrayDataSerializerSnapshot(eleType, eleSer);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link ArrayDataSerializer}.
	 */
	public static final class ArrayDataSerializerSnapshot implements TypeSerializerSnapshot<ArrayData> {
		private static final int CURRENT_VERSION = 3;

		private LogicalType previousType;
		private TypeSerializer previousEleSer;

		@SuppressWarnings("unused")
		public ArrayDataSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		ArrayDataSerializerSnapshot(LogicalType eleType, TypeSerializer eleSer) {
			this.previousType = eleType;
			this.previousEleSer = eleSer;
		}

		@Override
		public int getCurrentVersion() {
			return CURRENT_VERSION;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			DataOutputViewStream outStream = new DataOutputViewStream(out);
			InstantiationUtil.serializeObject(outStream, previousType);
			InstantiationUtil.serializeObject(outStream, previousEleSer);
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
			try {
				DataInputViewStream inStream = new DataInputViewStream(in);
				this.previousType = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
				this.previousEleSer = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		@Override
		public TypeSerializer<ArrayData> restoreSerializer() {
			return new ArrayDataSerializer(previousType, previousEleSer);
		}

		@Override
		public TypeSerializerSchemaCompatibility<ArrayData> resolveSchemaCompatibility(TypeSerializer<ArrayData> newSerializer) {
			if (!(newSerializer instanceof ArrayDataSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			ArrayDataSerializer newArrayDataSerializer = (ArrayDataSerializer) newSerializer;
			if (!previousType.equals(newArrayDataSerializer.eleType) ||
				!previousEleSer.equals(newArrayDataSerializer.eleSer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
