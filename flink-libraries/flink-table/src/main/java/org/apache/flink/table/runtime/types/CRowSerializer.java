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

package org.apache.flink.table.runtime.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * For CRow Types Serializer.
 */
@PublicEvolving
public class CRowSerializer extends TypeSerializer<CRow> {
	TypeSerializer<Row> rowSerializer;

	public CRowSerializer(TypeSerializer<Row> rowSerializer) {
		this.rowSerializer = rowSerializer;
	}

	public CRowSerializer() {
		this(null);
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<CRow> duplicate() {
		return new CRowSerializer(rowSerializer.duplicate());
	}

	@Override
	public CRow createInstance() {
		return new CRow(rowSerializer.createInstance(), true);
	}

	@Override
	public CRow copy(CRow from) {
		return new CRow(rowSerializer.copy(from.row), from.change);
	}

	@Override
	public CRow copy(CRow from, CRow reuse) {
		rowSerializer.copy(from.row, reuse.row);
		reuse.change = from.change;
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(CRow record, DataOutputView target) throws IOException {
		rowSerializer.serialize(record.row, target);
		target.writeBoolean(record.change);
	}

	@Override
	public CRow deserialize(DataInputView source) throws IOException {
		Row row = rowSerializer.deserialize(source);
		Boolean change = source.readBoolean();
		return new CRow(row, change);
	}

	@Override
	public CRow deserialize(CRow reuse, DataInputView source) throws IOException {
		rowSerializer.deserialize(reuse.row, source);
		reuse.change = source.readBoolean();
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		rowSerializer.copy(source, target);
		target.writeBoolean(source.readBoolean());
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj.getClass().equals(CRowSerializer.class);
	}

	@Override
	public boolean equals(Object obj) {
		if (canEqual(obj)) {
			CRowSerializer other = (CRowSerializer) obj;
			return rowSerializer.equals(other.rowSerializer);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return rowSerializer.hashCode() * 13;
	}

	@Override
	public TypeSerializerSnapshot<CRow> snapshotConfiguration() {
		return new CRowSerializer.CRowSerializerConfigSnapshot(rowSerializer);
	}

	@Override
	public CompatibilityResult<CRow> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
		if (configSnapshot.getClass().equals(CRowSerializer.CRowSerializerConfigSnapshot.class)) {
			CRowSerializerConfigSnapshot crowSerializerConfigSnapshot = (CRowSerializer.CRowSerializerConfigSnapshot) configSnapshot;
			CompatibilityResult<Row> compatResult = CompatibilityUtil.resolveCompatibilityResult(
				crowSerializerConfigSnapshot.getSingleNestedSerializerAndConfig().f0,
				UnloadableDummyTypeSerializer.class,
				crowSerializerConfigSnapshot.getSingleNestedSerializerAndConfig().f1,
				rowSerializer);

			if (compatResult.isRequiresMigration()) {
				if (compatResult.getConvertDeserializer() != null) {
					CompatibilityResult.requiresMigration(
						new CRowSerializer(
							new TypeDeserializerAdapter(compatResult.getConvertDeserializer()))
					);
				} else {
					CompatibilityResult.requiresMigration();
				}
			} else {
				CompatibilityResult.compatible();
			}
		} else {
			CompatibilityResult.requiresMigration();
		}
		return super.ensureCompatibility(configSnapshot);
	}

	/**
	 * To get CRow Serializer ConfigSnapshot.
	 */
	public void getCRowSerializerConfigSnapshot() {
		new CRowSerializerConfigSnapshot();
	}

	/**
	 * The CRow Serializer ConfigSnapshot.
	 */
	class CRowSerializerConfigSnapshot extends CompositeTypeSerializerConfigSnapshot<CRow> {

		private int version = 1;

		public CRowSerializerConfigSnapshot() {
			version = 1;
		}

		public CRowSerializerConfigSnapshot(TypeSerializer<Row>... rowSerializers) {
			super(rowSerializers);
		}

		@Override
		public int getVersion() {
			return this.version;
		}
	}
}
