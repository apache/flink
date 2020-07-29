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

package org.apache.flink.table.format.single;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Serialization schema that serializes an object of Flink internal data structure into a SINGLE-VALUE bytes.
 */
@Internal
public class SingleValueRowDataSerialization implements SerializationSchema<RowData> {

	private RowType rowType;

	private SerializationRuntimeConverter converter;

	private FieldGetter fieldGetter;

	public SingleValueRowDataSerialization(RowType rowType) {
		this.rowType = rowType;
		this.fieldGetter = RowData.createFieldGetter(rowType.getTypeAt(0), 0);
		this.converter = createConverter(rowType.getTypeAt(0));
	}

	@Override
	public byte[] serialize(RowData element) {
		return converter.convert(fieldGetter.getFieldOrNull(element));
	}

	/**
	 *  Runtime converter that convert a single value to byte[].
	 */
	@FunctionalInterface
	private interface SerializationRuntimeConverter extends Serializable {
		byte[] convert(Object object);
	}

	/**
	 *  Creates a runtime converter.
	 */
	private SerializationRuntimeConverter createConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return value -> ((StringData) value).toBytes();
			case VARBINARY:
			case BINARY:
				return value -> (byte[]) value;
			case TINYINT:
				return value -> ByteBuffer.allocate(Byte.BYTES).put((Byte) value).array();
			case SMALLINT:
				return value -> ByteBuffer.allocate(Short.BYTES).putShort((Short) value).array();
			case INTEGER:
				return value -> ByteBuffer.allocate(Integer.BYTES).putInt((Integer) value).array();
			case BIGINT:
				return value -> ByteBuffer.allocate(Long.BYTES).putLong((Long) value).array();
			case FLOAT:
				return value -> ByteBuffer.allocate(Float.BYTES).putFloat((Float) value).array();
			case DOUBLE:
				return value -> ByteBuffer.allocate(Double.BYTES).putDouble((Double) value).array();
			case BOOLEAN:
				return value -> ByteBuffer.allocate(Byte.BYTES).put(
					(byte) ((Boolean) value ? 1 : 0)).array();
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
