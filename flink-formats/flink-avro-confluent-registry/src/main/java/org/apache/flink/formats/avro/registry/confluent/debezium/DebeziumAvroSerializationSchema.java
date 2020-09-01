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

package org.apache.flink.formats.avro.registry.confluent.debezium;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
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
 * Serialization schema from Flink Table/SQL internal data structure {@link RowData} to Debezium Avro.
 */
public class DebeziumAvroSerializationSchema implements SerializationSchema<RowData> {
	private static final long serialVersionUID = 1L;

	/**
	 * insert operation.
	 */
	private static final StringData OP_CREATE = StringData.fromString("c");
	/**
	 * delete operation.
	 */
	private static final StringData OP_DELETE = StringData.fromString("d");

	/**
	 * The deserializer to deserialize Debezium Avro data.
	 */
	private final AvroRowDataSerializationSchema avroSerializer;

	public DebeziumAvroSerializationSchema(
			RowType rowType,
			String schemaRegistryUrl,
			String schemaRegistrySubject) {
		RowType debeziumAvroRowType = createDebeziumAvroRowType(fromLogicalToDataType(rowType));

		this.avroSerializer = new AvroRowDataSerializationSchema(
			debeziumAvroRowType,
			ConfluentRegistryAvroSerializationSchema.forGeneric(
				schemaRegistrySubject,
				AvroSchemaConverter.convertToSchema(debeziumAvroRowType),
				schemaRegistryUrl),
			RowDataToAvroConverters.createRowConverter(debeziumAvroRowType));
	}

	public DebeziumAvroSerializationSchema(AvroRowDataSerializationSchema avroSerializer) {
		this.avroSerializer = avroSerializer;
	}

	@Override
	public void open(InitializationContext context) throws Exception {
	}

	@Override
	public byte[] serialize(RowData element) {
		GenericRowData reuse = new GenericRowData(3);
		switch (element.getRowKind()) {
			case INSERT:
				reuse.setField(1, element);
				break;
			case DELETE:
				reuse.setField(0, element);
				break;
			default:
				throw new UnsupportedOperationException("Unsupported operation '" + element.getRowKind() + "' for row kind.");
		}
		reuse.setField(2, rowKind2String(element.getRowKind()));
		return avroSerializer.serialize(reuse);
	}

	private StringData rowKind2String(RowKind rowKind) {
		switch (rowKind) {
			case INSERT:
			case UPDATE_AFTER:
				return OP_CREATE;
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
		DebeziumAvroSerializationSchema that = (DebeziumAvroSerializationSchema) o;
		return Objects.equals(avroSerializer, that.avroSerializer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(avroSerializer);
	}

	public static RowType createDebeziumAvroRowType(DataType dataType) {
		// Debezium Avro contains other information, e.g. "source", "ts_ms"
		// but we don't need them
		return (RowType) DataTypes.ROW(
			DataTypes.FIELD("before", dataType.nullable()),
			DataTypes.FIELD("after", dataType.nullable()),
			DataTypes.FIELD("op", DataTypes.STRING())).getLogicalType();
	}

}
