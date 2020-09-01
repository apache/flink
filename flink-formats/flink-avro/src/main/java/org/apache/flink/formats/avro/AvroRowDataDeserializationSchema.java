/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.Objects;

/**
 * Deserialization schema from Avro bytes to {@link RowData}.
 *
 * <p>Deserializes the <code>byte[]</code> messages into (nested) Flink RowData. It converts Avro types
 * into types that are compatible with Flink's Table & SQL API.
 *
 * <p>Projects with Avro records containing logical date/time types need to add a JodaTime
 * dependency.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime
 * class {@link AvroRowDataSerializationSchema} and schema converter {@link AvroSchemaConverter}.
 */
@PublicEvolving
public class AvroRowDataDeserializationSchema implements DeserializationSchema<RowData> {

	private static final long serialVersionUID = 1L;

	/** Nested schema to deserialize the inputs into avro {@link GenericRecord}. **/
	private final DeserializationSchema<GenericRecord> nestedSchema;

	/**
	 * Type information describing the result type.
	 */
	private final TypeInformation<RowData> typeInfo;

	/**
	 * Runtime instance that performs the actual work.
	 */
	private final AvroToRowDataConverters.AvroToRowDataConverter runtimeConverter;

	/**
	 * Creates a Avro deserialization schema for the given logical type.
	 *
	 * @param rowType  The logical type used to deserialize the data.
	 * @param typeInfo The TypeInformation to be used by {@link AvroRowDataDeserializationSchema#getProducedType()}.
	 */
	public AvroRowDataDeserializationSchema(RowType rowType, TypeInformation<RowData> typeInfo) {
		this(
				AvroDeserializationSchema
						.forGeneric(AvroSchemaConverter.convertToSchema(rowType)),
				AvroToRowDataConverters.createRowConverter(rowType),
				typeInfo);
	}

	/**
	 * Creates a Avro deserialization schema for the given logical type.
	 *
	 * @param nestedSchema     Deserialization schema to deserialize as {@link GenericRecord}
	 * @param runtimeConverter Converter that transforms a {@link GenericRecord} into {@link RowData}
	 * @param typeInfo         The TypeInformation to be used by
	 *                         {@link AvroRowDataDeserializationSchema#getProducedType()}
	 */
	public AvroRowDataDeserializationSchema(
			DeserializationSchema<GenericRecord> nestedSchema,
			AvroToRowDataConverters.AvroToRowDataConverter runtimeConverter,
			TypeInformation<RowData> typeInfo) {
		this.nestedSchema = nestedSchema;
		this.typeInfo = typeInfo;
		this.runtimeConverter = runtimeConverter;
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		this.nestedSchema.open(context);
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		try {
			GenericRecord deserialize = nestedSchema.deserialize(message);
			return (RowData) runtimeConverter.convert(deserialize);
		} catch (Exception e) {
			throw new IOException("Failed to deserialize Avro record.", e);
		}
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return typeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		AvroRowDataDeserializationSchema that = (AvroRowDataDeserializationSchema) o;
		return nestedSchema.equals(that.nestedSchema) &&
				typeInfo.equals(that.typeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(nestedSchema, typeInfo);
	}
}
