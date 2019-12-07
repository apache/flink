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
import org.apache.flink.table.dataformat.BaseArray;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.BinaryArrayWriter;
import org.apache.flink.table.dataformat.BinaryWriter;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * A {@link TypeSerializer} for {@link BaseArray}. It should be noted that the header will not be
 * encoded. Currently Python doesn't support BinaryArray natively, so we can't use BaseArraySerializer
 * in blink directly.
 */
@Internal
public class BaseArraySerializer extends org.apache.flink.table.runtime.typeutils.BaseArraySerializer {

	private static final long serialVersionUID = 1L;

	private final LogicalType elementType;

	private final TypeSerializer elementTypeSerializer;

	private final int elementSize;

	public BaseArraySerializer(LogicalType eleType, TypeSerializer elementTypeSerializer) {
		super(eleType, null);
		this.elementType = eleType;
		this.elementTypeSerializer = elementTypeSerializer;
		this.elementSize = BinaryArray.calculateFixLengthPartSize(this.elementType);
	}

	@Override
	public void serialize(BaseArray array, DataOutputView target) throws IOException {
		int len = array.numElements();
		target.writeInt(len);
		for (int i = 0; i < len; i++) {
			if (array.isNullAt(i)) {
				target.writeBoolean(false);
			} else {
				target.writeBoolean(true);
				Object element = TypeGetterSetters.get(array, i, elementType);
				elementTypeSerializer.serialize(element, target);
			}
		}
	}

	@Override
	public BaseArray deserialize(DataInputView source) throws IOException {
		BinaryArray array = new BinaryArray();
		deserializeInternal(source, array);
		return array;
	}

	@Override
	public BaseArray deserialize(BaseArray reuse, DataInputView source) throws IOException {
		return deserializeInternal(source, toBinaryArray(reuse));
	}

	private BaseArray deserializeInternal(DataInputView source, BinaryArray array) throws IOException {
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
	public TypeSerializer<BaseArray> duplicate() {
		return new BaseArraySerializer(elementType, elementTypeSerializer);
	}

	@Override
	public TypeSerializerSnapshot<BaseArray> snapshotConfiguration() {
		return new BaseArraySerializerSnapshot(elementType, elementTypeSerializer);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BaseArraySerializer}.
	 */
	public static final class BaseArraySerializerSnapshot implements TypeSerializerSnapshot<BaseArray> {
		private static final int CURRENT_VERSION = 1;

		private LogicalType previousType;
		private TypeSerializer previousEleSer;

		@SuppressWarnings("unused")
		public BaseArraySerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		BaseArraySerializerSnapshot(LogicalType eleType, TypeSerializer eleSer) {
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
		public TypeSerializer<BaseArray> restoreSerializer() {
			return new BaseArraySerializer(previousType, previousEleSer);
		}

		@Override
		public TypeSerializerSchemaCompatibility<BaseArray> resolveSchemaCompatibility(TypeSerializer<BaseArray> newSerializer) {
			if (!(newSerializer instanceof BaseArraySerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			BaseArraySerializer newBaseArraySerializer = (BaseArraySerializer) newSerializer;
			if (!previousType.equals(newBaseArraySerializer.elementType) ||
				!previousEleSer.equals(newBaseArraySerializer.elementTypeSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
