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
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;

/**
 * Serializer for {@link RowData}.
 */
@Internal
public class RowDataSerializer extends AbstractRowDataSerializer<RowData> {
	private static final long serialVersionUID = 1L;

	private BinaryRowDataSerializer binarySerializer;
	private final LogicalType[] types;
	private final TypeSerializer[] fieldSerializers;

	private transient BinaryRowData reuseRow;
	private transient BinaryRowWriter reuseWriter;

	public RowDataSerializer(RowType rowType) {
		this(rowType.getChildren().toArray(new LogicalType[0]),
			rowType.getChildren().stream()
				.map(InternalSerializers::create)
				.toArray(TypeSerializer[]::new));
	}

	public RowDataSerializer(LogicalType... types) {
		this(types, Arrays.stream(types)
			.map(InternalSerializers::create)
			.toArray(TypeSerializer[]::new));
	}

	public RowDataSerializer(LogicalType[] types, TypeSerializer<?>[] fieldSerializers) {
		this.types = types;
		this.fieldSerializers = fieldSerializers;
		this.binarySerializer = new BinaryRowDataSerializer(types.length);
	}

	@Override
	public TypeSerializer<RowData> duplicate() {
		TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
		for (int i = 0; i < fieldSerializers.length; i++) {
			duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
		}
		return new RowDataSerializer(types, duplicateFieldSerializers);
	}

	@Override
	public RowData createInstance() {
		// default use binary row to deserializer
		return new BinaryRowData(types.length);
	}

	@Override
	public void serialize(RowData row, DataOutputView target) throws IOException {
		binarySerializer.serialize(toBinaryRow(row), target);
	}

	@Override
	public RowData deserialize(DataInputView source) throws IOException {
		return binarySerializer.deserialize(source);
	}

	@Override
	public RowData deserialize(RowData reuse, DataInputView source) throws IOException {
		if (reuse instanceof BinaryRowData) {
			return binarySerializer.deserialize((BinaryRowData) reuse, source);
		} else {
			return binarySerializer.deserialize(source);
		}
	}

	@Override
	public RowData copy(RowData from) {
		if (from.getArity() != types.length) {
			throw new IllegalArgumentException("Row arity: " + from.getArity() +
				", but serializer arity: " + types.length);
		}
		if (from instanceof BinaryRowData) {
			return ((BinaryRowData) from).copy();
		} else {
			return copyRowData(from, new GenericRowData(from.getArity()));
		}
	}

	@Override
	public RowData copy(RowData from, RowData reuse) {
		if (from.getArity() != types.length || reuse.getArity() != types.length) {
			throw new IllegalArgumentException("Row arity: " + from.getArity() +
				", reuse Row arity: " + reuse.getArity() +
				", but serializer arity: " + types.length);
		}
		if (from instanceof BinaryRowData) {
			return reuse instanceof BinaryRowData
				? ((BinaryRowData) from).copy((BinaryRowData) reuse)
				: ((BinaryRowData) from).copy();
		} else {
			return copyRowData(from, reuse);
		}
	}

	@SuppressWarnings("unchecked")
	private RowData copyRowData(RowData from, RowData reuse) {
		GenericRowData ret;
		if (reuse instanceof GenericRowData) {
			ret = (GenericRowData) reuse;
		} else {
			ret = new GenericRowData(from.getArity());
		}
		ret.setRowKind(from.getRowKind());
		for (int i = 0; i < from.getArity(); i++) {
			if (!from.isNullAt(i)) {
				ret.setField(
					i,
					fieldSerializers[i].copy((RowData.get(from, i, types[i])))
				);
			} else {
				ret.setField(i, null);
			}
		}
		return ret;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		binarySerializer.copy(source, target);
	}

	@Override
	public int getArity() {
		return types.length;
	}

	/**
	 * Convert {@link RowData} into {@link BinaryRowData}.
	 * TODO modify it to code gen.
	 */
	@Override
	public BinaryRowData toBinaryRow(RowData row) {
		if (row instanceof BinaryRowData) {
			return (BinaryRowData) row;
		}
		if (reuseRow == null) {
			reuseRow = new BinaryRowData(types.length);
			reuseWriter = new BinaryRowWriter(reuseRow);
		}
		reuseWriter.reset();
		reuseWriter.writeRowKind(row.getRowKind());
		for (int i = 0; i < types.length; i++) {
			if (row.isNullAt(i)) {
				reuseWriter.setNullAt(i);
			} else {
				BinaryWriter.write(reuseWriter, i, RowData.get(row, i, types[i]), types[i], fieldSerializers[i]);
			}
		}
		reuseWriter.complete();
		return reuseRow;
	}

	@Override
	public int serializeToPages(RowData row, AbstractPagedOutputView target) throws IOException {
		return binarySerializer.serializeToPages(toBinaryRow(row), target);
	}

	@Override
	public RowData deserializeFromPages(AbstractPagedInputView source) throws IOException {
		throw new UnsupportedOperationException("Not support!");
	}

	@Override
	public RowData deserializeFromPages(
		RowData reuse,
		AbstractPagedInputView source) throws IOException {
		throw new UnsupportedOperationException("Not support!");
	}

	@Override
	public RowData mapFromPages(AbstractPagedInputView source) throws IOException {
		//noinspection unchecked
		return binarySerializer.mapFromPages(source);
	}

	@Override
	public RowData mapFromPages(
		RowData reuse,
		AbstractPagedInputView source) throws IOException {
		if (reuse instanceof BinaryRowData) {
			return binarySerializer.mapFromPages((BinaryRowData) reuse, source);
		} else {
			throw new UnsupportedOperationException("Not support!");
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RowDataSerializer) {
			RowDataSerializer other = (RowDataSerializer) obj;
			return Arrays.equals(fieldSerializers, other.fieldSerializers);
		}

		return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(fieldSerializers);
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public TypeSerializerSnapshot<RowData> snapshotConfiguration() {
		return new RowDataSerializerSnapshot(types, fieldSerializers);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BinaryRowDataSerializer}.
	 */
	public static final class RowDataSerializerSnapshot implements TypeSerializerSnapshot<RowData> {
		private static final int CURRENT_VERSION = 3;

		private LogicalType[] previousTypes;
		private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

		@SuppressWarnings("unused")
		public RowDataSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		RowDataSerializerSnapshot(LogicalType[] types, TypeSerializer[] serializers) {
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
				}
				catch (ClassNotFoundException e) {
					throw new IOException(e);
				}
			}
			this.nestedSerializersSnapshotDelegate = NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
				in,
				userCodeClassLoader
			);
		}

		@Override
		public RowDataSerializer restoreSerializer() {
			return new RowDataSerializer(
				previousTypes,
				nestedSerializersSnapshotDelegate.getRestoredNestedSerializers()
			);
		}

		@Override
		public TypeSerializerSchemaCompatibility<RowData> resolveSchemaCompatibility(TypeSerializer<RowData> newSerializer) {
			if (!(newSerializer instanceof RowDataSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			RowDataSerializer newRowSerializer = (RowDataSerializer) newSerializer;
			if (!Arrays.equals(previousTypes, newRowSerializer.types)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			CompositeTypeSerializerUtil.IntermediateCompatibilityResult<RowData> intermediateResult =
				CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
					newRowSerializer.fieldSerializers,
					nestedSerializersSnapshotDelegate.getNestedSerializerSnapshots()
				);

			if (intermediateResult.isCompatibleWithReconfiguredSerializer()) {
				RowDataSerializer reconfiguredCompositeSerializer = restoreSerializer();
				return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
					reconfiguredCompositeSerializer);
			}

			return intermediateResult.getFinalResult();
		}
	}
}
