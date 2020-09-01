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

package org.apache.flink.formats.json.canal;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Objects;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Serialization schema that serializes an object of Flink Table/SQL internal data structure {@link RowData} into a Canal JSON bytes.
 *
 * @see <a href="https://github.com/alibaba/canal">Alibaba Canal</a>
 */
public class CanalJsonSerializationSchema implements SerializationSchema<RowData> {

	private static final long serialVersionUID = 1L;

	private static final StringData OP_INSERT = StringData.fromString("INSERT");
	private static final StringData OP_DELETE = StringData.fromString("DELETE");

	private transient GenericRowData reuse;

	/**
	 * The serializer to serialize Canal JSON data.
	 */
	private final JsonRowDataSerializationSchema jsonSerializer;

	public CanalJsonSerializationSchema(RowType rowType, TimestampFormat timestampFormat) {
		jsonSerializer = new JsonRowDataSerializationSchema(
			createJsonRowType(fromLogicalToDataType(rowType)),
			timestampFormat);
	}

	@Override
	public void open(InitializationContext context) {
		reuse = new GenericRowData(2);
	}

	@Override
	public byte[] serialize(RowData row) {
		try {
			StringData opType = rowKind2String(row.getRowKind());
			ArrayData arrayData = new GenericArrayData(new RowData[]{row});
			reuse.setField(0, arrayData);
			reuse.setField(1, opType);
			return jsonSerializer.serialize(reuse);
		} catch (Throwable t) {
			throw new RuntimeException("Could not serialize row '" + row + "'.", t);
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
		CanalJsonSerializationSchema that = (CanalJsonSerializationSchema) o;
		return Objects.equals(jsonSerializer, that.jsonSerializer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(jsonSerializer);
	}

	private static RowType createJsonRowType(DataType databaseSchema) {
		// Canal JSON contains other information, e.g. "database", "ts"
		// but we don't need them
		// and we don't need "old" , because can not support UPDATE_BEFORE,UPDATE_AFTER
		return (RowType) DataTypes.ROW(
			DataTypes.FIELD("data", DataTypes.ARRAY(databaseSchema)),
			DataTypes.FIELD("type", DataTypes.STRING())).getLogicalType();
	}
}
