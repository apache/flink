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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.Objects;

/**
 * Serialization schema that serializes an object of Flink internal data structure into a
 * SINGLE-VALUE bytes.
 */
@Internal
public class SingleValueRowDataSerialization implements SerializationSchema<RowData> {

	private RowType rowType;

	private SerializationRuntimeConverter converter;

	private FieldGetter fieldGetter;

	private MemorySegment segment;

	public SingleValueRowDataSerialization(RowType rowType) {
		this.rowType = rowType;
		this.fieldGetter = RowData.createFieldGetter(rowType.getTypeAt(0), 0);
		this.converter = createConverter(rowType.getTypeAt(0));
		this.segment = createSegment(rowType.getTypeAt(0));
	}

	@Override
	public byte[] serialize(RowData element) {
		return converter.convert(segment, fieldGetter.getFieldOrNull(element));
	}

	/**
	 * Runtime converter that convert a single value to byte[].
	 */
	@FunctionalInterface
	private interface SerializationRuntimeConverter extends Serializable {

		byte[] convert(MemorySegment segment, Object object);
	}

	/**
	 * Creates a runtime converter.
	 */
	private SerializationRuntimeConverter createConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return (segment, value) -> ((StringData) value).toBytes();
			case VARBINARY:
			case BINARY:
				return (segment, value) -> (byte[]) value;
			case TINYINT:
				return (segment, value) -> {
					segment.put(0, (byte) value);
					return segment.getArray();
				};
			case SMALLINT:
				return (segment, value) -> {
					segment.putShort(0, (short) value);
					return segment.getArray();
				};
			case INTEGER:
				return (segment, value) -> {
					segment.putInt(0, (int) value);
					return segment.getArray();
				};
			case BIGINT:
				return (segment, value) -> {
					segment.putLong(0, (long) value);
					return segment.getArray();
				};
			case FLOAT:
				return (segment, value) -> {
					segment.putFloat(0, (float) value);
					return segment.getArray();
				};
			case DOUBLE:
				return (segment, value) -> {
					segment.putDouble(0, (double) value);
					return segment.getArray();
				};
			case BOOLEAN:
				return (segment, value) -> {
					segment.putBoolean(0, (boolean) value);
					return segment.getArray();
				};
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	/**
	 * Creates a memory segment.
	 */
	private MemorySegment createSegment(LogicalType type) {
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
			case VARBINARY:
			case BINARY:
				return MemorySegmentFactory.wrap(new byte[0]);
			case TINYINT:
				return MemorySegmentFactory.wrap(new byte[Byte.BYTES]);
			case SMALLINT:
				return MemorySegmentFactory.wrap(new byte[Short.BYTES]);
			case INTEGER:
				return MemorySegmentFactory.wrap(new byte[Integer.BYTES]);
			case BIGINT:
				return MemorySegmentFactory.wrap(new byte[Long.BYTES]);
			case FLOAT:
				return MemorySegmentFactory.wrap(new byte[Float.BYTES]);
			case DOUBLE:
				return MemorySegmentFactory.wrap(new byte[Double.BYTES]);
			case BOOLEAN:
				return MemorySegmentFactory.wrap(new byte[1]);
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
		SingleValueRowDataSerialization that = (SingleValueRowDataSerialization) o;
		return rowType.equals(that.rowType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(rowType);
	}
}
