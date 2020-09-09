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

package org.apache.flink.formats.json.debezium;

import org.apache.flink.api.common.serialization.SerializationSchema;
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

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Serialization schema from Flink Table/SQL internal data structure {@link RowData} to Debezium JSON.
 *
 * @see <a href="https://debezium.io/">Debezium</a>
 */
public class DebeziumJsonSerializationSchema implements SerializationSchema<RowData> {
	private static final long serialVersionUID = 1L;

	private static final StringData OP_INSERT = StringData.fromString("c"); // insert
	private static final StringData OP_DELETE = StringData.fromString("d"); // delete

	/** The serializer to serialize Debezium JSON data. **/
	private final JsonRowDataSerializationSchema jsonSerializer;

	private transient GenericRowData genericRowData;

	public DebeziumJsonSerializationSchema(RowType rowType, TimestampFormat timestampFormat) {
		jsonSerializer = new JsonRowDataSerializationSchema(
			createJsonRowType(fromLogicalToDataType(rowType)),
			timestampFormat);
	}

	@Override
	public void open(InitializationContext context) {
		genericRowData = new GenericRowData(3);
	}

	@Override
	public byte[] serialize(RowData rowData) {
		try {
			if (RowKind.INSERT == rowData.getRowKind()) {
				genericRowData.setField(0,  null);
				genericRowData.setField(1, rowData);
			} else if (RowKind.DELETE == rowData.getRowKind()) {
				genericRowData.setField(0,  rowData);
				genericRowData.setField(1, null);
			}
			genericRowData.setField(2, rowKind2String(rowData.getRowKind()));
			return jsonSerializer.serialize(genericRowData);
		} catch (Throwable t) {
			throw new RuntimeException(format("Could not serialize row '%s'.", rowData), t);
		}
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
				throw new UnsupportedOperationException(format("Unsupported operation '%s' for row kind.", rowKind));
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
		DebeziumJsonSerializationSchema that = (DebeziumJsonSerializationSchema) o;
		return Objects.equals(jsonSerializer, that.jsonSerializer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(jsonSerializer);
	}

	private static RowType createJsonRowType(DataType databaseSchema) {
		// Debezium JSON contains some other information, e.g. "source", "ts_ms"
		// but we don't need them.
		return (RowType) DataTypes.ROW(
			DataTypes.FIELD("before", databaseSchema),
			DataTypes.FIELD("after", databaseSchema),
			DataTypes.FIELD("op", DataTypes.STRING())).getLogicalType();
	}
}
