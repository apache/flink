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

package org.apache.flink.table.formats;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Deserialization schema from SINGLE-VALUE to Flink Table/SQL internal data structure {@link RowData}.
 */
@Internal
public class SingleValueRowDataDeserialization implements DeserializationSchema<RowData> {

	private DeserializationRuntimeConverter converter;
	private RowType rowType;
	private TypeInformation<RowData> typeInfo;

	public SingleValueRowDataDeserialization(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo) {
		this.rowType = rowType;
		this.typeInfo = resultTypeInfo;
		this.converter = createConverter(rowType.getTypeAt(0));
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		GenericRowData genericRowData = new GenericRowData(1);
		genericRowData.setField(0, converter.convert(HeapMemorySegment.FACTORY.wrap(message)));
		return genericRowData;
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return typeInfo;
	}

	/**
	 * Runtime converter that convert byte[] to a single value.
	 */
	@FunctionalInterface
	private interface DeserializationRuntimeConverter extends Serializable {
		Object convert(HeapMemorySegment segment);
	}

	/**
	 *  Creates a runtime converter.
	 */
	private DeserializationRuntimeConverter createConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return segment -> StringData.fromBytes(segment.getArray());
			case VARBINARY:
			case BINARY:
				return segment -> segment.getArray();
			case TINYINT:
				return segment -> segment.get(0);
			case SMALLINT:
				return segment -> segment.getShortLittleEndian(0);
			case INTEGER:
				return segment -> segment.getIntLittleEndian(0);
			case BIGINT:
				return segment -> segment.getLong(0);
			case FLOAT:
				return segment -> segment.getFloat(0);
			case DOUBLE:
				return segment -> segment.getDouble(0);
			case BOOLEAN:
				return segment -> segment.getBoolean(0);
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SingleValueRowDataDeserialization that = (SingleValueRowDataDeserialization) o;
		return typeInfo.equals(that.typeInfo) &&
				rowType.equals(that.rowType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(typeInfo, rowType);
	}
}
