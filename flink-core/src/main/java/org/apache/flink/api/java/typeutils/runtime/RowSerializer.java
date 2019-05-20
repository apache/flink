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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;

import static org.apache.flink.api.java.typeutils.runtime.NullMaskUtils.readIntoAndCopyNullMask;
import static org.apache.flink.api.java.typeutils.runtime.NullMaskUtils.readIntoNullMask;
import static org.apache.flink.api.java.typeutils.runtime.NullMaskUtils.writeNullMask;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Serializer for {@link Row}.
 */
@Internal
public final class RowSerializer extends TypeSerializer<Row> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<Object>[] fieldSerializers;

	private final int arity;

	private transient boolean[] nullMask;

	@SuppressWarnings("unchecked")
	public RowSerializer(TypeSerializer<?>[] fieldSerializers) {
		this.fieldSerializers = (TypeSerializer<Object>[]) checkNotNull(fieldSerializers);
		this.arity = fieldSerializers.length;
		this.nullMask = new boolean[fieldSerializers.length];
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<Row> duplicate() {
		TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
		for (int i = 0; i < fieldSerializers.length; i++) {
			duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
		}
		return new RowSerializer(duplicateFieldSerializers);
	}

	@Override
	public Row createInstance() {
		return new Row(fieldSerializers.length);
	}

	@Override
	public Row copy(Row from) {
		int len = fieldSerializers.length;

		if (from.getArity() != len) {
			throw new RuntimeException("Row arity of from does not match serializers.");
		}

		Row result = new Row(len);
		for (int i = 0; i < len; i++) {
			Object fromField = from.getField(i);
			if (fromField != null) {
				Object copy = fieldSerializers[i].copy(fromField);
				result.setField(i, copy);
			}
			else {
				result.setField(i, null);
			}
		}
		return result;
	}

	@Override
	public Row copy(Row from, Row reuse) {
		int len = fieldSerializers.length;

		// cannot reuse, do a non-reuse copy
		if (reuse == null) {
			return copy(from);
		}

		if (from.getArity() != len || reuse.getArity() != len) {
			throw new RuntimeException(
				"Row arity of reuse or from is incompatible with this RowSerializer.");
		}

		for (int i = 0; i < len; i++) {
			Object fromField = from.getField(i);
			if (fromField != null) {
				Object reuseField = reuse.getField(i);
				if (reuseField != null) {
					Object copy = fieldSerializers[i].copy(fromField, reuseField);
					reuse.setField(i, copy);
				}
				else {
					Object copy = fieldSerializers[i].copy(fromField);
					reuse.setField(i, copy);
				}
			}
			else {
				reuse.setField(i, null);
			}
		}
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	public int getArity() {
		return arity;
	}

	@Override
	public void serialize(Row record, DataOutputView target) throws IOException {
		int len = fieldSerializers.length;

		if (record.getArity() != len) {
			throw new RuntimeException("Row arity of from does not match serializers.");
		}

		// write a null mask
		writeNullMask(len, record, target);

		// serialize non-null fields
		for (int i = 0; i < len; i++) {
			Object o = record.getField(i);
			if (o != null) {
				fieldSerializers[i].serialize(o, target);
			}
		}
	}

	@Override
	public Row deserialize(DataInputView source) throws IOException {
		int len = fieldSerializers.length;

		Row result = new Row(len);

		// read null mask
		readIntoNullMask(len, source, nullMask);

		for (int i = 0; i < len; i++) {
			if (nullMask[i]) {
				result.setField(i, null);
			}
			else {
				result.setField(i, fieldSerializers[i].deserialize(source));
			}
		}

		return result;
	}

	@Override
	public Row deserialize(Row reuse, DataInputView source) throws IOException {
		int len = fieldSerializers.length;

		if (reuse.getArity() != len) {
			throw new RuntimeException("Row arity of from does not match serializers.");
		}

		// read null mask
		readIntoNullMask(len, source, nullMask);

		for (int i = 0; i < len; i++) {
			if (nullMask[i]) {
				reuse.setField(i, null);
			}
			else {
				Object reuseField = reuse.getField(i);
				if (reuseField != null) {
					reuse.setField(i, fieldSerializers[i].deserialize(reuseField, source));
				}
				else {
					reuse.setField(i, fieldSerializers[i].deserialize(source));
				}
			}
		}

		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int len = fieldSerializers.length;

		// copy null mask
		readIntoAndCopyNullMask(len, source, target, nullMask);

		for (int i = 0; i < len; i++) {
			if (!nullMask[i]) {
				fieldSerializers[i].copy(source, target);
			}
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RowSerializer) {
			RowSerializer other = (RowSerializer) obj;
			if (this.fieldSerializers.length == other.fieldSerializers.length) {
				for (int i = 0; i < this.fieldSerializers.length; i++) {
					if (!this.fieldSerializers[i].equals(other.fieldSerializers[i])) {
						return false;
					}
				}
				return true;
			}
		}

		return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(fieldSerializers);
	}

	// --------------------------------------------------------------------------------------------

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		this.nullMask = new boolean[fieldSerializers.length];
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshoting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public TypeSerializerSnapshot<Row> snapshotConfiguration() {
		return new RowSerializerSnapshot(this);
	}

	/**
	 * A snapshot for {@link RowSerializer}.
	 *
	 * @deprecated this snapshot class is no longer in use, and is maintained only for backwards compatibility.
	 *             It is fully replaced by {@link RowSerializerSnapshot}.
	 */
	@Deprecated
	public static final class RowSerializerConfigSnapshot extends CompositeTypeSerializerConfigSnapshot<Row> {

		private static final int VERSION = 1;

		/**
		 * This empty nullary constructor is required for deserializing the configuration.
		 */
		public RowSerializerConfigSnapshot() {
		}

		public RowSerializerConfigSnapshot(TypeSerializer[] fieldSerializers) {
			super(fieldSerializers);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public TypeSerializerSchemaCompatibility<Row> resolveSchemaCompatibility(TypeSerializer<Row> newSerializer) {
			TypeSerializerSnapshot<?>[] nestedSnapshots = getNestedSerializersAndConfigs()
				.stream()
				.map(t -> t.f1)
				.toArray(TypeSerializerSnapshot[]::new);

			return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
				newSerializer,
				new RowSerializerSnapshot(),
				nestedSnapshots);
		}
	}

	/**
	 * A {@link TypeSerializerSnapshot} for RowSerializer.
	 */
	public static final class RowSerializerSnapshot extends CompositeTypeSerializerSnapshot<Row, RowSerializer> {

		private static final int VERSION = 2;

		@SuppressWarnings("WeakerAccess")
		public RowSerializerSnapshot() {
			super(RowSerializer.class);
		}

		RowSerializerSnapshot(RowSerializer serializerInstance) {
			super(serializerInstance);
		}

		@Override
		protected int getCurrentOuterSnapshotVersion() {
			return VERSION;
		}

		@Override
		protected TypeSerializer<?>[] getNestedSerializers(RowSerializer outerSerializer) {
			return outerSerializer.fieldSerializers;
		}

		@Override
		protected RowSerializer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
			return new RowSerializer(nestedSerializers);
		}
	}
}
