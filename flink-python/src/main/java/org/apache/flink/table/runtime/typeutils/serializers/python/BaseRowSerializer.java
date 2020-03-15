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
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.api.java.typeutils.runtime.NullMaskUtils.readIntoNullMask;

/**
 * A {@link TypeSerializer} for {@link BaseRow}. It should be noted that the header will not be encoded.
 * Currently Python doesn't support BaseRow natively, so we can't use BaseRowSerializer in blink directly.
 */
@Internal
public class BaseRowSerializer extends org.apache.flink.table.runtime.typeutils.BaseRowSerializer {

	private final LogicalType[] fieldTypes;

	private final TypeSerializer[] fieldSerializers;

	private transient boolean[] nullMask;

	public BaseRowSerializer(LogicalType[] types, TypeSerializer[] fieldSerializers) {
		super(types, fieldSerializers);
		this.fieldTypes = types;
		this.fieldSerializers = fieldSerializers;
		this.nullMask = new boolean[fieldTypes.length];
	}

	@Override
	public void serialize(BaseRow row, DataOutputView target) throws IOException {
		int len = fieldSerializers.length;

		if (row.getArity() != len) {
			throw new RuntimeException("Row arity of input element does not match serializers.");
		}

		// write a null mask
		writeNullMask(len, row, target);

		for (int i = 0; i < row.getArity(); i++) {
			if (!row.isNullAt(i)) {
				// TODO: support BaseRow natively in Python, then we can eliminate the redundant serialize/deserialize
				fieldSerializers[i].serialize(TypeGetterSetters.get(row, i, fieldTypes[i]), target);
			}
		}
	}

	@Override
	public BaseRow deserialize(DataInputView source) throws IOException {
		int len = fieldSerializers.length;

		// read null mask
		readIntoNullMask(len, source, nullMask);

		GenericRow row = new GenericRow(fieldSerializers.length);
		for (int i = 0; i < row.getArity(); i++) {
			if (nullMask[i]) {
				row.setField(i, null);
			} else {
				row.setField(i, fieldSerializers[i].deserialize(source));
			}
		}
		return row;
	}

	@Override
	public BaseRow deserialize(BaseRow reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		serialize(deserialize(source), target);
	}

	private static void writeNullMask(int len, BaseRow value, DataOutputView target) throws IOException {
		int b = 0x00;
		int bytePos = 0;

		int fieldPos = 0;
		int numPos = 0;
		while (fieldPos < len) {
			b = 0x00;
			// set bits in byte
			bytePos = 0;
			numPos = Math.min(8, len - fieldPos);
			while (bytePos < numPos) {
				b = b << 1;
				// set bit if field is null
				if (value.isNullAt(fieldPos + bytePos)) {
					b |= 0x01;
				}
				bytePos += 1;
			}
			fieldPos += numPos;
			// shift bits if last byte is not completely filled
			b <<= (8 - bytePos);
			// write byte
			target.writeByte(b);
		}
	}

	@Override
	public TypeSerializerSnapshot<BaseRow> snapshotConfiguration() {
		return new BaseRowSerializerSnapshot(fieldTypes, fieldSerializers);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BaseRowSerializer}.
	 */
	public static final class BaseRowSerializerSnapshot implements TypeSerializerSnapshot<BaseRow> {
		private static final int CURRENT_VERSION = 3;

		private LogicalType[] previousTypes;
		private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

		@SuppressWarnings("unused")
		public BaseRowSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		BaseRowSerializerSnapshot(LogicalType[] types, TypeSerializer[] serializers) {
			this.previousTypes = types;
			this.nestedSerializersSnapshotDelegate = new NestedSerializersSnapshotDelegate(
				serializers);
		}

		@Override
		public int getCurrentVersion() {
			return CURRENT_VERSION;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			out.writeInt(previousTypes.length);
			DataOutputViewStream stream = new DataOutputViewStream(out);
			for (LogicalType previousType : previousTypes) {
				InstantiationUtil.serializeObject(stream, previousType);
			}
			nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
			throws IOException {
			int length = in.readInt();
			DataInputViewStream stream = new DataInputViewStream(in);
			previousTypes = new LogicalType[length];
			for (int i = 0; i < length; i++) {
				try {
					previousTypes[i] = InstantiationUtil.deserializeObject(
						stream,
						userCodeClassLoader
					);
				} catch (ClassNotFoundException e) {
					throw new IOException(e);
				}
			}
			this.nestedSerializersSnapshotDelegate = NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
				in,
				userCodeClassLoader
			);
		}

		@Override
		public BaseRowSerializer restoreSerializer() {
			return new BaseRowSerializer(
				previousTypes,
				nestedSerializersSnapshotDelegate.getRestoredNestedSerializers()
			);
		}

		@Override
		public TypeSerializerSchemaCompatibility<BaseRow> resolveSchemaCompatibility(TypeSerializer<BaseRow> newSerializer) {
			if (!(newSerializer instanceof BaseRowSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			BaseRowSerializer newRowSerializer = (BaseRowSerializer) newSerializer;
			if (!Arrays.equals(previousTypes, newRowSerializer.fieldTypes)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			CompositeTypeSerializerUtil.IntermediateCompatibilityResult<BaseRow> intermediateResult =
				CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
					newRowSerializer.fieldSerializers,
					nestedSerializersSnapshotDelegate.getNestedSerializerSnapshots()
				);

			if (intermediateResult.isCompatibleWithReconfiguredSerializer()) {
				BaseRowSerializer reconfiguredCompositeSerializer = restoreSerializer();
				return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
					reconfiguredCompositeSerializer);
			}

			return intermediateResult.getFinalResult();
		}
	}
}
