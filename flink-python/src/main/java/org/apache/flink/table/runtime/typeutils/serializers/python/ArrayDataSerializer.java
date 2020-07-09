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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * A {@link TypeSerializer} for {@link ArrayData}. It should be noted that the header will not be
 * encoded. Currently Python doesn't support BinaryArrayData natively, so we can't use BaseArraySerializer
 * in blink directly.
 */
@Internal
public class ArrayDataSerializer extends org.apache.flink.table.runtime.typeutils.ArrayDataSerializer {

	private static final long serialVersionUID = 1L;

	private final LogicalType elementType;

	private final TypeSerializer elementTypeSerializer;

	private final int elementSize;

	public ArrayDataSerializer(LogicalType eleType, TypeSerializer elementTypeSerializer) {
		super(eleType);
		this.elementType = eleType;
		this.elementTypeSerializer = elementTypeSerializer;
		this.elementSize = BinaryArrayData.calculateFixLengthPartSize(this.elementType);
	}

	@Override
	public void serialize(ArrayData array, DataOutputView target) throws IOException {
		int len = array.size();
		target.writeInt(len);
		for (int i = 0; i < len; i++) {
			if (array.isNullAt(i)) {
				target.writeBoolean(false);
			} else {
				target.writeBoolean(true);
				Object element = ArrayData.get(array, i, elementType);
				elementTypeSerializer.serialize(element, target);
			}
		}
	}

	@Override
	public ArrayData deserialize(DataInputView source) throws IOException {
		BinaryArrayData array = new BinaryArrayData();
		deserializeInternal(source, array);
		return array;
	}

	@Override
	public ArrayData deserialize(ArrayData reuse, DataInputView source) throws IOException {
		return deserializeInternal(source, toBinaryArray(reuse));
	}

	private ArrayData deserializeInternal(DataInputView source, BinaryArrayData array) throws IOException {
		int len = source.readInt();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, len, elementSize);
		for (int i = 0; i < len; i++) {
			boolean isNonNull = source.readBoolean();
			if (isNonNull) {
				Object element = elementTypeSerializer.deserialize(source);
				BinaryWriter.write(writer, i, element, elementType, elementTypeSerializer);
			} else {
				writer.setNullAt(i);
			}
		}
		writer.complete();
		return array;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		serialize(deserialize(source), target);
	}

	@Override
	public TypeSerializer<ArrayData> duplicate() {
		return new ArrayDataSerializer(elementType, elementTypeSerializer);
	}

	@Override
	public TypeSerializerSnapshot<ArrayData> snapshotConfiguration() {
		return new ArrayDataSerializerSnapshot(elementType, elementTypeSerializer);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link ArrayDataSerializer}.
	 */
	public static final class ArrayDataSerializerSnapshot implements TypeSerializerSnapshot<ArrayData> {
		private static final int CURRENT_VERSION = 1;

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
			if (!previousType.equals(newArrayDataSerializer.elementType) ||
				!previousEleSer.equals(newArrayDataSerializer.elementTypeSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
