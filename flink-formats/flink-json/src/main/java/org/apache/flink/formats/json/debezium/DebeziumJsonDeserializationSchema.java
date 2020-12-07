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
import org.apache.flink.formats.json.debezium.DebeziumJsonDecodingFormat.ReadableMetadata;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;

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

	/** The deserializer to deserialize Debezium JSON data. */
	private final JsonRowDataDeserializationSchema jsonDeserializer;

	/** Flag that indicates that an additional projection is required for metadata. */
	private final boolean hasMetadata;

	/** Metadata to be extracted for every record. */
	private final MetadataConverter[] metadataConverters;

	/** {@link TypeInformation} of the produced {@link RowData} (physical + meta data). */
	private final TypeInformation<RowData> producedTypeInfo;

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
			DataType physicalDataType,
			List<ReadableMetadata> requestedMetadata,
			TypeInformation<RowData> producedTypeInfo,
			boolean schemaInclude,
			boolean ignoreParseErrors,
			TimestampFormat timestampFormat) {
		final RowType jsonRowType = createJsonRowType(physicalDataType, requestedMetadata, schemaInclude);
		this.jsonDeserializer = new JsonRowDataDeserializationSchema(
			jsonRowType,
			// the result type is never used, so it's fine to pass in the produced type info
			producedTypeInfo,
			false, // ignoreParseErrors already contains the functionality of failOnMissingField
			ignoreParseErrors,
			timestampFormat);
		this.hasMetadata = requestedMetadata.size() > 0;
		this.metadataConverters = createMetadataConverters(jsonRowType, requestedMetadata, schemaInclude);
		this.producedTypeInfo = producedTypeInfo;
		this.schemaInclude = schemaInclude;
		this.ignoreParseErrors = ignoreParseErrors;
	}

	@Override
	public RowData deserialize(byte[] message) {
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
				emitRow(row, after, out);
			} else if (OP_UPDATE.equals(op)) {
				if (before == null) {
					throw new IllegalStateException(String.format(REPLICA_IDENTITY_EXCEPTION, "UPDATE"));
				}
				before.setRowKind(RowKind.UPDATE_BEFORE);
				after.setRowKind(RowKind.UPDATE_AFTER);
				emitRow(row, before, out);
				emitRow(row, after, out);
			} else if (OP_DELETE.equals(op)) {
				if (before == null) {
					throw new IllegalStateException(String.format(REPLICA_IDENTITY_EXCEPTION, "DELETE"));
				}
				before.setRowKind(RowKind.DELETE);
				emitRow(row, before, out);
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

	private void emitRow(GenericRowData rootRow, GenericRowData physicalRow, Collector<RowData> out) {
		// shortcut in case no output projection is required
		if (!hasMetadata) {
			out.collect(physicalRow);
			return;
		}

		final int physicalArity = physicalRow.getArity();
		final int metadataArity = metadataConverters.length;

		final GenericRowData producedRow = new GenericRowData(
					physicalRow.getRowKind(),
					physicalArity + metadataArity);

		for (int physicalPos = 0; physicalPos < physicalArity; physicalPos++) {
			producedRow.setField(physicalPos, physicalRow.getField(physicalPos));
		}

		for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
			producedRow.setField(physicalArity + metadataPos, metadataConverters[metadataPos].convert(rootRow));
		}

		out.collect(producedRow);
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
		DebeziumJsonDeserializationSchema that = (DebeziumJsonDeserializationSchema) o;
		return Objects.equals(jsonDeserializer, that.jsonDeserializer)
				&& hasMetadata == that.hasMetadata
				&& Objects.equals(producedTypeInfo, that.producedTypeInfo)
				&& schemaInclude == that.schemaInclude
				&& ignoreParseErrors == that.ignoreParseErrors;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
				jsonDeserializer,
				hasMetadata,
				producedTypeInfo,
				schemaInclude,
				ignoreParseErrors);
	}

	// --------------------------------------------------------------------------------------------

	private static RowType createJsonRowType(
			DataType physicalDataType,
			List<ReadableMetadata> readableMetadata,
			boolean schemaInclude) {
		DataType payload = DataTypes.ROW(
			DataTypes.FIELD("before", physicalDataType),
			DataTypes.FIELD("after", physicalDataType),
			DataTypes.FIELD("op", DataTypes.STRING()));

		// append fields that are required for reading metadata in the payload
		final List<DataTypes.Field> payloadMetadataFields = readableMetadata.stream()
				.filter(m -> m.isJsonPayload)
				.map(m -> m.requiredJsonField)
				.distinct()
				.collect(Collectors.toList());
		payload = DataTypeUtils.appendRowFields(payload, payloadMetadataFields);

		DataType root = payload;
		if (schemaInclude) {
			// when Debezium Kafka connect enables "value.converter.schemas.enable",
			// the JSON will contain "schema" information and we need to extract data from "payload".
			root = DataTypes.ROW(DataTypes.FIELD("payload", payload));
		}

		// append fields that are required for reading metadata in the root
		final List<DataTypes.Field> rootMetadataFields = readableMetadata.stream()
				.filter(m -> !m.isJsonPayload)
				.map(m -> m.requiredJsonField)
				.distinct()
				.collect(Collectors.toList());
		root = DataTypeUtils.appendRowFields(root, rootMetadataFields);

		return (RowType) root.getLogicalType();
	}

	private static MetadataConverter[] createMetadataConverters(
			RowType jsonRowType,
			List<ReadableMetadata> requestedMetadata,
			boolean schemaInclude) {
		return requestedMetadata.stream()
				.map(m -> {
					if (m.isJsonPayload) {
						return convertInPayload(jsonRowType, m, schemaInclude);
					} else {
						return convertInRoot(jsonRowType, m);
					}
				})
				.toArray(MetadataConverter[]::new);
	}

	private static MetadataConverter convertInRoot(
			RowType jsonRowType,
			ReadableMetadata metadata) {
		final int pos = findFieldPos(metadata, jsonRowType);
		return new MetadataConverter() {
			private static final long serialVersionUID = 1L;
			@Override
			public Object convert(GenericRowData root, int unused) {
				return metadata.converter.convert(root, pos);
			}
		};
	}

	private static MetadataConverter convertInPayload(
			RowType jsonRowType,
			ReadableMetadata metadata,
			boolean schemaInclude) {
		if (schemaInclude) {
			final int pos = findFieldPos(metadata, (RowType) jsonRowType.getChildren().get(0));
			return new MetadataConverter() {
				private static final long serialVersionUID = 1L;
				@Override
				public Object convert(GenericRowData root, int unused) {
					final GenericRowData payload = (GenericRowData) root.getField(0);
					return metadata.converter.convert(payload, pos);
				}
			};
		}
		return convertInRoot(jsonRowType, metadata);
	}

	private static int findFieldPos(ReadableMetadata metadata, RowType jsonRowType) {
		return jsonRowType.getFieldNames().indexOf(metadata.requiredJsonField.getName());
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Converter that extracts a metadata field from the row (root or payload) that comes out of the
	 * JSON schema and converts it to the desired data type.
	 */
	interface MetadataConverter extends Serializable {

		// Method for top-level access.
		default Object convert(GenericRowData row) {
			return convert(row, -1);
		}

		Object convert(GenericRowData row, int pos);
	}
}
