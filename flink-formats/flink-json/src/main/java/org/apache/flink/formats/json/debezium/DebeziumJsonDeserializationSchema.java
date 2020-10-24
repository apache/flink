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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
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
 * Deserialization schema from Debezium JSON to Flink Table/SQL internal data structure {@link RowData}.
 * The deserialization schema knows Debezium's schema definition and can extract the database data
 * and convert into {@link RowData} with {@link RowKind}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads
 * the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 *
 * @see <a href="https://debezium.io/">Debezium</a>
 */
@Internal
public final class DebeziumJsonDeserializationSchema implements DeserializationSchema<RowData> {
	private static final long serialVersionUID = 1L;

	private static final String OP_READ = "r"; // snapshot read
	private static final String OP_CREATE = "c"; // insert
	private static final String OP_UPDATE = "u"; // update
	private static final String OP_DELETE = "d"; // delete

	private static final String REPLICA_IDENTITY_EXCEPTION = "The \"before\" field of %s message is null, " +
		"if you are using Debezium Postgres Connector, " +
		"please check the Postgres table has been set REPLICA IDENTITY to FULL level.";

	/** The deserializer to deserialize Debezium JSON data. **/
	private final JsonRowDataDeserializationSchema jsonDeserializer;

	/** TypeInformation of the produced {@link RowData}. **/
	private final TypeInformation<RowData> resultTypeInfo;

	/**
	 * Flag indicating whether the Debezium JSON data contains schema part or not.
	 * When Debezium Kafka Connect enables "value.converter.schemas.enable", the JSON
	 * will contain "schema" information, but we just ignore "schema" and extract data
	 * from "payload".
	 */
	private final boolean schemaInclude;

	/** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
	private final boolean ignoreParseErrors;

	public DebeziumJsonDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			boolean schemaInclude,
			boolean ignoreParseErrors,
			TimestampFormat timestampFormatOption) {
		this.resultTypeInfo = resultTypeInfo;
		this.schemaInclude = schemaInclude;
		this.ignoreParseErrors = ignoreParseErrors;
		this.jsonDeserializer = new JsonRowDataDeserializationSchema(
			createJsonRowType(fromLogicalToDataType(rowType), schemaInclude),
			// the result type is never used, so it's fine to pass in Debezium's result type
			resultTypeInfo,
			false, // ignoreParseErrors already contains the functionality of failOnMissingField
			ignoreParseErrors,
			timestampFormatOption);
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
			GenericRowData row = (GenericRowData) jsonDeserializer.deserialize(message);
			GenericRowData payload;
			if (schemaInclude) {
				payload = (GenericRowData) row.getField(0);
			} else {
				payload = row;
			}

			GenericRowData before = (GenericRowData) payload.getField(0);
			GenericRowData after = (GenericRowData) payload.getField(1);
			String op = payload.getField(2).toString();
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
				if (!ignoreParseErrors) {
					throw new IOException(format(
						"Unknown \"op\" value \"%s\". The Debezium JSON message is '%s'", op, new String(message)));
				}
			}
		} catch (Throwable t) {
			// a big try catch to protect the processing.
			if (!ignoreParseErrors) {
				throw new IOException(format(
					"Corrupt Debezium JSON message '%s'.", new String(message)), t);
			}
		}
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return resultTypeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DebeziumJsonDeserializationSchema that = (DebeziumJsonDeserializationSchema) o;
		return schemaInclude == that.schemaInclude &&
			ignoreParseErrors == that.ignoreParseErrors &&
			Objects.equals(jsonDeserializer, that.jsonDeserializer) &&
			Objects.equals(resultTypeInfo, that.resultTypeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(jsonDeserializer, resultTypeInfo, schemaInclude, ignoreParseErrors);
	}

	private static RowType createJsonRowType(DataType databaseSchema, boolean schemaInclude) {
		DataType payload = DataTypes.ROW(
			DataTypes.FIELD("before", databaseSchema),
			DataTypes.FIELD("after", databaseSchema),
			DataTypes.FIELD("op", DataTypes.STRING()));
		if (schemaInclude) {
			// when Debezium Kafka connect enables "value.converter.schemas.enable",
			// the JSON will contain "schema" information, but we just ignore "schema"
			// and extract data from "payload".
			return (RowType) DataTypes.ROW(
				DataTypes.FIELD("payload", payload)).getLogicalType();
		} else {
			// payload contains some other information, e.g. "source", "ts_ms"
			// but we don't need them.
			return (RowType) payload.getLogicalType();
		}
	}
}
