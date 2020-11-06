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

package org.apache.flink.formats.json.maxwell;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Objects;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Serialization schema from Flink Table/SQL internal data structure {@link RowData} to maxwell-Json.
 */
public class MaxwellJsonSerializationSchema implements SerializationSchema<RowData> {
	private static final long serialVersionUID = 1L;

	private static final StringData OP_INSERT = StringData.fromString("insert");
	private static final StringData OP_DELETE = StringData.fromString("delete");

	private final JsonRowDataSerializationSchema jsonSerializer;

	/**
	 * Timestamp format specification which is used to parse timestamp.
	 */
	private final TimestampFormat timestampFormat;

	private transient GenericRowData reuse;

	public MaxwellJsonSerializationSchema(
			RowType rowType,
			TimestampFormat timestampFormat,
			JsonOptions.MapNullKeyMode mapNullKeyMode,
			String mapNullKeyLiteral) {
		this.jsonSerializer = new JsonRowDataSerializationSchema(
				createJsonRowType(fromLogicalToDataType(rowType)),
				timestampFormat,
				mapNullKeyMode,
				mapNullKeyLiteral);
		this.timestampFormat = timestampFormat;
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		this.reuse = new GenericRowData(2);
	}

	@Override
	public byte[] serialize(RowData element) {
		reuse.setField(0, element);
		reuse.setField(1, rowKind2String(element.getRowKind()));
		return jsonSerializer.serialize(reuse);
	}

	private StringData rowKind2String(RowKind rowKind) {
		switch (rowKind) {
			case INSERT:
			case UPDATE_AFTER:
				return OP_INSERT;
			case UPDATE_BEFORE:
			case DELETE:
				return OP_DELETE;
			default:
				throw new UnsupportedOperationException("Unsupported operation '" + rowKind + "' for row kind.");
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
		MaxwellJsonSerializationSchema that = (MaxwellJsonSerializationSchema) o;
		return Objects.equals(jsonSerializer, that.jsonSerializer) &&
			timestampFormat == that.timestampFormat;
	}

	@Override
	public int hashCode() {
		return Objects.hash(jsonSerializer, timestampFormat);
	}

	private RowType createJsonRowType(DataType databaseSchema) {
		DataType payload = DataTypes.ROW(
			DataTypes.FIELD("data", databaseSchema),
			DataTypes.FIELD("type", DataTypes.STRING()));
		return (RowType) payload.getLogicalType();
	}

}
