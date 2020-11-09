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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Objects;

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Deserialization schema from Debezium Avro to Flink Table/SQL internal data structure {@link RowData}.
 * The deserialization schema knows Debezium's schema definition and can extract the database data
 * and convert into {@link RowData} with {@link RowKind}.
 * Deserializes a <code>byte[]</code> message as a JSON object and reads
 * the specified fields.
 * Failures during deserialization are forwarded as wrapped IOExceptions.
 *
 * @see <a href="https://debezium.io/">Debezium</a>
 */
@Internal
public final class DebeziumAvroDeserializationSchema implements DeserializationSchema<RowData> {
	private static final long serialVersionUID = 1L;

	/**
	 * snapshot read.
	 */
	private static final String OP_READ = "r";
	/**
	 * insert operation.
	 */
	private static final String OP_CREATE = "c";
	/**
	 * update operation.
	 */
	private static final String OP_UPDATE = "u";
	/**
	 * delete operation.
	 */
	private static final String OP_DELETE = "d";

	private static final String REPLICA_IDENTITY_EXCEPTION = "The \"before\" field of %s message is null, " +
		"if you are using Debezium Postgres Connector, " +
		"please check the Postgres table has been set REPLICA IDENTITY to FULL level.";

	/**
	 * The deserializer to deserialize Debezium Avro data.
	 */
	private final AvroRowDataDeserializationSchema avroDeserializer;

	/**
	 * TypeInformation of the produced {@link RowData}.
	 **/
	private final TypeInformation<RowData> producedTypeInfo;

	public DebeziumAvroDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> producedTypeInfo,
			String schemaRegistryUrl) {
		this.producedTypeInfo = producedTypeInfo;
		RowType debeziumAvroRowType = createDebeziumAvroRowType(
			fromLogicalToDataType(rowType));

		this.avroDeserializer = new AvroRowDataDeserializationSchema(
			ConfluentRegistryAvroDeserializationSchema.forGeneric(
				AvroSchemaConverter.convertToSchema(debeziumAvroRowType),
				schemaRegistryUrl),
			AvroToRowDataConverters.createRowConverter(debeziumAvroRowType),
			producedTypeInfo);
	}

	@VisibleForTesting
	DebeziumAvroDeserializationSchema(
			TypeInformation<RowData> producedTypeInfo,
			AvroRowDataDeserializationSchema avroDeserializer) {
		this.producedTypeInfo = producedTypeInfo;
		this.avroDeserializer = avroDeserializer;
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		avroDeserializer.open(context);
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		throw new RuntimeException(
			"Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
	}

	@Override
	public void deserialize(byte[] message, Collector<RowData> out) throws IOException {

		if (message == null || message.length == 0) {
			// skip tombstone messages
			return;
		}
		try {
			GenericRowData row = (GenericRowData) avroDeserializer.deserialize(message);

			GenericRowData before = (GenericRowData) row.getField(0);
			GenericRowData after = (GenericRowData) row.getField(1);
			String op = row.getField(2).toString();
			if (OP_CREATE.equals(op) || OP_READ.equals(op)) {
				after.setRowKind(RowKind.INSERT);
				out.collect(after);
			} else if (OP_UPDATE.equals(op)) {
				if (before == null) {
					throw new IllegalStateException(String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
				}
				before.setRowKind(RowKind.UPDATE_BEFORE);
				after.setRowKind(RowKind.UPDATE_AFTER);
				out.collect(before);
				out.collect(after);
			} else if (OP_DELETE.equals(op)) {
				if (before == null) {
					throw new IllegalStateException(String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
				}
				before.setRowKind(RowKind.DELETE);
				out.collect(before);
			} else {
				throw new IOException(format(
					"Unknown \"op\" value \"%s\". The Debezium Avro message is '%s'", op, new String(message)));
			}
		} catch (Throwable t) {
			// a big try catch to protect the processing.
			throw new IOException("Can't deserialize Debezium Avro message.", t);
		}
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return producedTypeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DebeziumAvroDeserializationSchema that = (DebeziumAvroDeserializationSchema) o;
		return Objects.equals(avroDeserializer, that.avroDeserializer) &&
			Objects.equals(producedTypeInfo, that.producedTypeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(avroDeserializer, producedTypeInfo);
	}

	public static RowType createDebeziumAvroRowType(DataType databaseSchema) {
		// Debezium Avro contains other information, e.g. "source", "ts_ms"
		// but we don't need them
		return (RowType) DataTypes.ROW(
			DataTypes.FIELD("before", databaseSchema.nullable()),
			DataTypes.FIELD("after", databaseSchema.nullable()),
			DataTypes.FIELD("op", DataTypes.STRING())).getLogicalType();
	}
}
