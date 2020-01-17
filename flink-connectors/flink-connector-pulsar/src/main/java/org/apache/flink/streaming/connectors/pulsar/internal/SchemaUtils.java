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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.BooleanSchema;
import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.DateSchema;
import org.apache.pulsar.client.impl.schema.DoubleSchema;
import org.apache.pulsar.client.impl.schema.FloatSchema;
import org.apache.pulsar.client.impl.schema.IntSchema;
import org.apache.pulsar.client.impl.schema.LongSchema;
import org.apache.pulsar.client.impl.schema.ShortSchema;
import org.apache.pulsar.client.impl.schema.TimeSchema;
import org.apache.pulsar.client.impl.schema.TimestampSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableList;
import org.apache.pulsar.shade.com.google.common.collect.ImmutableSet;
import org.apache.pulsar.shade.org.apache.avro.LogicalType;
import org.apache.pulsar.shade.org.apache.avro.LogicalTypes;
import org.apache.pulsar.shade.org.apache.avro.Schema;
import org.apache.pulsar.shade.org.apache.avro.SchemaBuilder;
import scala.NotImplementedError;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.EVENT_TIME_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.KEY_ATTRIBUTE_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.MESSAGE_ID_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.PUBLISH_TIME_NAME;
import static org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions.TOPIC_ATTRIBUTE_NAME;
import static org.apache.flink.util.Preconditions.checkNotNull;


/**
 * Utility class that enables type conversions between Pulsar schema and Flink type system.
 */
public class SchemaUtils {

	public static TableSchema toTableSchema(FieldsDataType schema) {
		RowType rt = (RowType) schema.getLogicalType();
		List<DataType> fieldTypes = rt.getFieldNames().stream().map(fn -> schema.getFieldDataTypes().get(fn)).collect(Collectors.toList());

		return TableSchema.builder().fields(
			rt.getFieldNames().toArray(new String[0]), fieldTypes.toArray(new DataType[0])).build();
	}

	public static void uploadPulsarSchema(PulsarAdmin admin, String topic, SchemaInfo schemaInfo) {
		checkNotNull(schemaInfo);

		SchemaInfo existingSchema;
		try {
			existingSchema = admin.schemas().getSchemaInfo(TopicName.get(topic).toString());
		} catch (PulsarAdminException pae) {
			if (pae.getStatusCode() == 404) {
				existingSchema = null;
			} else {
				throw new RuntimeException(
					String.format("Failed to get schema information for %s", TopicName.get(topic).toString()), pae);
			}
		} catch (Throwable e) {
			throw new RuntimeException(
				String.format("Failed to get schema information for %s", TopicName.get(topic).toString()), e);
		}

		if (existingSchema == null) {
			PostSchemaPayload pl = new PostSchemaPayload();
			pl.setType(schemaInfo.getType().name());
			pl.setSchema(new String(schemaInfo.getSchema(), StandardCharsets.UTF_8));
			pl.setProperties(schemaInfo.getProperties());

			try {
				admin.schemas().createSchema(TopicName.get(topic).toString(), pl);
			} catch (PulsarAdminException pae) {
				if (pae.getStatusCode() == 404) {
					throw new RuntimeException(
						String.format("Create schema for %s get 404", TopicName.get(topic).toString()), pae);
				} else {
					throw new RuntimeException(
						String.format("Failed to create schema information for %s", TopicName.get(topic).toString()), pae);
				}
			} catch (Throwable e) {
				throw new RuntimeException(
					String.format("Failed to create schema information for %s", TopicName.get(topic).toString()), e);
			}
		} else if (!existingSchema.equals(schemaInfo) && !compatibleSchema(existingSchema, schemaInfo)) {
			throw new RuntimeException("Writing to a topic which have incompatible schema");
		}
	}

	public static boolean compatibleSchema(SchemaInfo s1, SchemaInfo s2) {
		if (s1.getType() == SchemaType.NONE && s2.getType() == SchemaType.BYTES) {
			return true;
		} else return s1.getType() == SchemaType.BYTES && s2.getType() == SchemaType.NONE;
	}

	public static org.apache.pulsar.client.api.Schema<?> getPulsarSchema(SchemaInfo schemaInfo) {
		switch (schemaInfo.getType()) {
			case BOOLEAN:
				return BooleanSchema.of();
			case INT8:
				return ByteSchema.of();
			case INT16:
				return ShortSchema.of();
			case INT32:
				return IntSchema.of();
			case INT64:
				return LongSchema.of();
			case STRING:
				return org.apache.pulsar.client.api.Schema.STRING;
			case FLOAT:
				return FloatSchema.of();
			case DOUBLE:
				return DoubleSchema.of();
			case BYTES:
			case NONE:
				return BytesSchema.of();
			case DATE:
				return DateSchema.of();
			case TIME:
				return TimeSchema.of();
			case TIMESTAMP:
				return TimestampSchema.of();
			case AVRO:
			case JSON:
				return GenericSchemaImpl.of(schemaInfo);
			default:
				throw new IllegalArgumentException("Retrieve schema instance from schema info for type " +
					schemaInfo.getType() + " is not supported yet");
		}
	}

	public static FieldsDataType pulsarSourceSchema(SchemaInfo si) throws IncompatibleSchemaException {
		List<DataTypes.Field> mainSchema = new ArrayList<>();
		DataType dataType = si2SqlType(si);
		if (dataType instanceof FieldsDataType) {
			FieldsDataType fieldsDataType = (FieldsDataType) dataType;
			RowType rowType = (RowType) fieldsDataType.getLogicalType();
			rowType.getFieldNames().stream()
				.map(fieldName -> DataTypes.FIELD(fieldName, fieldsDataType.getFieldDataTypes().get(fieldName)))
				.forEach(mainSchema::add);
		} else {
			mainSchema.add(DataTypes.FIELD("value", dataType));
		}

		mainSchema.addAll(metadataFields);
		return (FieldsDataType) DataTypes.ROW(mainSchema.toArray(new DataTypes.Field[0]));
	}

	public static List<DataTypes.Field> metadataFields = ImmutableList.of(
		DataTypes.FIELD(
			KEY_ATTRIBUTE_NAME,
			DataTypes.BYTES()),
		DataTypes.FIELD(
			TOPIC_ATTRIBUTE_NAME,
			DataTypes.STRING()),
		DataTypes.FIELD(
			MESSAGE_ID_NAME,
			DataTypes.BYTES()),
		DataTypes.FIELD(
			PUBLISH_TIME_NAME,
			DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class)),
		DataTypes.FIELD(
			EVENT_TIME_NAME,
			DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class)));

	public static DataType si2SqlType(SchemaInfo si) throws IncompatibleSchemaException, NotImplementedError {
		switch (si.getType()) {
			case NONE:
			case BYTES:
				return DataTypes.BYTES();
			case BOOLEAN:
				return DataTypes.BOOLEAN();
			case DATE:
				return DataTypes.DATE();
			case STRING:
				return DataTypes.STRING();
			case TIMESTAMP:
				return DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class);
			case INT8:
				return DataTypes.TINYINT();
			case DOUBLE:
				return DataTypes.DOUBLE();
			case FLOAT:
				return DataTypes.FLOAT();
			case INT32:
				return DataTypes.INT();
			case INT64:
				return DataTypes.BIGINT();
			case INT16:
				return DataTypes.SMALLINT();
			case AVRO:
			case JSON:
				Schema avroSchema =
					new Schema.Parser().parse(new String(si.getSchema(), StandardCharsets.UTF_8));
				return avro2SqlType(avroSchema, Collections.emptySet());
			default:
				throw new NotImplementedError(String.format("We do not support %s currently.", si.getType()));
		}
	}

	private static DataType avro2SqlType(Schema avroSchema, Set<String> existingRecordNames) throws IncompatibleSchemaException {
		LogicalType logicalType = avroSchema.getLogicalType();
		switch (avroSchema.getType()) {
			case INT:
				if (logicalType instanceof LogicalTypes.Date) {
					return DataTypes.DATE();
				} else {
					return DataTypes.INT();
				}

			case STRING:
			case ENUM:
				return DataTypes.STRING();

			case BOOLEAN:
				return DataTypes.BOOLEAN();

			case BYTES:
			case FIXED:
				// For FIXED type, if the precision requires more bytes than fixed size, the logical
				// type will be null, which is handled by Avro library.
				if (logicalType instanceof LogicalTypes.Decimal) {
					LogicalTypes.Decimal d = (LogicalTypes.Decimal) logicalType;
					return DataTypes.DECIMAL(d.getPrecision(), d.getScale());
				} else {
					return DataTypes.BYTES();
				}

			case DOUBLE:
				return DataTypes.DOUBLE();

			case FLOAT:
				return DataTypes.FLOAT();

			case LONG:
				if (logicalType instanceof LogicalTypes.TimestampMillis ||
					logicalType instanceof LogicalTypes.TimestampMicros) {
					return DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class);
				} else {
					return DataTypes.BIGINT();
				}

			case RECORD:
				if (existingRecordNames.contains(avroSchema.getFullName())) {
					throw new IncompatibleSchemaException(
						String.format("Found recursive reference in Avro schema, which can not be processed by Flink: %s", avroSchema.toString(true)), null);
				}

				Set<String> newRecordName = ImmutableSet.<String>builder()
					.addAll(existingRecordNames).add(avroSchema.getFullName()).build();
				List<DataTypes.Field> fields = new ArrayList<>();
				for (Schema.Field f : avroSchema.getFields()) {
					DataType fieldType = avro2SqlType(f.schema(), newRecordName);
					fields.add(DataTypes.FIELD(f.name(), fieldType));
				}
				return DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));

			case ARRAY:
				DataType elementType = avro2SqlType(avroSchema.getElementType(), existingRecordNames);
				return DataTypes.ARRAY(elementType);

			case MAP:
				DataType valueType = avro2SqlType(avroSchema.getValueType(), existingRecordNames);
				return DataTypes.MAP(DataTypes.STRING(), valueType);

			case UNION:
				if (avroSchema.getTypes().stream().anyMatch(f -> f.getType() == Schema.Type.NULL)) {
					// In case of a union with null, eliminate it and make a recursive call
					List<Schema> remainingUnionTypes =
						avroSchema.getTypes().stream().filter(f -> f.getType() != Schema.Type.NULL).collect(Collectors.toList());
					if (remainingUnionTypes.size() == 1) {
						return avro2SqlType(remainingUnionTypes.get(0), existingRecordNames).nullable();
					} else {
						return avro2SqlType(Schema.createUnion(remainingUnionTypes), existingRecordNames).nullable();
					}
				} else {
					List<Schema.Type> types = avroSchema.getTypes().stream().map(Schema::getType).collect(Collectors.toList());
					if (types.size() == 1) {
						return avro2SqlType(avroSchema.getTypes().get(0), existingRecordNames);
					} else if (types.size() == 2 && types.contains(Schema.Type.INT) && types.contains(Schema.Type.LONG)) {
						return DataTypes.BIGINT();
					} else if (types.size() == 2 && types.contains(Schema.Type.FLOAT) && types.contains(Schema.Type.DOUBLE)) {
						return DataTypes.DOUBLE();
					} else {
						// Convert complex unions to struct types where field names are member0, member1, etc.
						// This is consistent with the behavior when converting between Avro and Parquet.
						List<DataTypes.Field> memberFields = new ArrayList<>();
						List<Schema> schemas = avroSchema.getTypes();
						for (int i = 0; i < schemas.size(); i++) {
							DataType memberType = avro2SqlType(schemas.get(i), existingRecordNames);
							memberFields.add(DataTypes.FIELD("member" + i, memberType));
						}
						return DataTypes.ROW(memberFields.toArray(new DataTypes.Field[0]));
					}
				}

			default:
				throw new IncompatibleSchemaException(String.format("Unsupported type %s", avroSchema.toString(true)), null);
		}
	}

	public static org.apache.pulsar.client.api.Schema sqlType2PulsarSchema(DataType flinkType) throws IncompatibleSchemaException {
		if (flinkType instanceof AtomicDataType) {
			LogicalTypeRoot type = flinkType.getLogicalType().getTypeRoot();
			switch (type) {
				case BOOLEAN:
					return BooleanSchema.of();
				case VARBINARY:
					return BytesSchema.of();
				case DATE:
					return DateSchema.of();
				case VARCHAR:
					return org.apache.pulsar.client.api.Schema.STRING;
				case TIMESTAMP_WITHOUT_TIME_ZONE:
					return TimestampSchema.of();
				case TINYINT:
					return ByteSchema.of();
				case DOUBLE:
					return DoubleSchema.of();
				case FLOAT:
					return FloatSchema.of();
				case INTEGER:
					return IntSchema.of();
				case BIGINT:
					return LongSchema.of();
				case SMALLINT:
					return ShortSchema.of();
				default:
					throw new IncompatibleSchemaException(String.format("%s is not supported by Pulsar yet", flinkType.toString()), null);
			}
		} else if (flinkType instanceof FieldsDataType) {
			return avroSchema2PulsarSchema(sqlType2AvroSchema(flinkType));
		}
		throw new IncompatibleSchemaException(String.format("%s is not supported by Pulsar yet", flinkType.toString()), null);
	}

	static GenericSchema<GenericRecord> avroSchema2PulsarSchema(Schema avroSchema) {
		byte[] schemaBytes = avroSchema.toString().getBytes(StandardCharsets.UTF_8);
		SchemaInfo si = new SchemaInfo();
		si.setName("Avro");
		si.setSchema(schemaBytes);
		si.setType(SchemaType.AVRO);
		return org.apache.pulsar.client.api.Schema.generic(si);
	}

	public static Schema sqlType2AvroSchema(DataType flinkType) throws IncompatibleSchemaException {
		return sqlType2AvroSchema(flinkType, false, "topLevelRecord", "");
	}

	private static Schema sqlType2AvroSchema(DataType flinkType, boolean nullable, String recordName, String namespace) throws IncompatibleSchemaException {
		SchemaBuilder.TypeBuilder<Schema> builder = SchemaBuilder.builder();
		LogicalTypeRoot type = flinkType.getLogicalType().getTypeRoot();
		Schema schema = null;

		if (flinkType instanceof AtomicDataType) {
			switch (type) {
				case BOOLEAN:
					schema = builder.booleanType();
					break;
				case TINYINT:
				case SMALLINT:
				case INTEGER:
					schema = builder.intType();
					break;
				case BIGINT:
					schema = builder.longType();
					break;
				case DATE:
					schema = LogicalTypes.date().addToSchema(builder.intType());
					break;
				case TIMESTAMP_WITHOUT_TIME_ZONE:
					schema = LogicalTypes.timestampMicros().addToSchema(builder.longType());
					break;
				case FLOAT:
					schema = builder.floatType();
					break;
				case DOUBLE:
					schema = builder.doubleType();
					break;
				case VARCHAR:
					schema = builder.stringType();
					break;
				case BINARY:
				case VARBINARY:
					schema = builder.bytesType();
					break;
				case DECIMAL:
					DecimalType dt = (DecimalType) flinkType.getLogicalType();
					LogicalTypes.Decimal avroType = LogicalTypes.decimal(dt.getPrecision(), dt.getScale());
					int fixedSize = minBytesForPrecision[dt.getPrecision()];
					// Need to avoid naming conflict for the fixed fields
					String name;
					if (namespace.equals("")) {
						name = recordName + ".fixed";
					} else {
						name = namespace + recordName + ".fixed";
					}
					schema = avroType.addToSchema(SchemaBuilder.fixed(name).size(fixedSize));
					break;
				default:
					throw new IncompatibleSchemaException(String.format("Unsupported type %s", flinkType.toString()), null);
			}
		} else if (flinkType instanceof CollectionDataType) {
			if (type == LogicalTypeRoot.ARRAY) {
				CollectionDataType cdt = (CollectionDataType) flinkType;
				DataType elementType = cdt.getElementDataType();
				schema = builder.array().items(sqlType2AvroSchema(elementType, elementType.getLogicalType().isNullable(), recordName, namespace));
			} else {
				throw new IncompatibleSchemaException("Pulsar only support collection as array", null);
			}
		} else if (flinkType instanceof KeyValueDataType) {
			KeyValueDataType kvType = (KeyValueDataType) flinkType;
			DataType keyType = kvType.getKeyDataType();
			DataType valueType = kvType.getValueDataType();
			if (!(keyType instanceof AtomicDataType) || keyType.getLogicalType().getTypeRoot() != LogicalTypeRoot.VARCHAR) {
				throw new IncompatibleSchemaException("Pulsar only support string key map", null);
			}
			schema = builder.map().values(sqlType2AvroSchema(valueType, valueType.getLogicalType().isNullable(), recordName, namespace));
		} else if (flinkType instanceof FieldsDataType) {
			FieldsDataType fieldsDataType = (FieldsDataType) flinkType;
			String childNamespace = namespace.equals("") ? recordName : namespace + "." + recordName;
			SchemaBuilder.FieldAssembler<Schema> fieldsAssembler = builder.record(recordName).namespace(namespace).fields();
			RowType rowType = (RowType) fieldsDataType.getLogicalType();

			for (String fieldName : rowType.getFieldNames()) {
				DataType ftype = fieldsDataType.getFieldDataTypes().get(fieldName);
				Schema fieldAvroSchema = sqlType2AvroSchema(ftype, ftype.getLogicalType().isNullable(), fieldName, childNamespace);
				fieldsAssembler.name(fieldName).type(fieldAvroSchema).noDefault();
			}
			schema = fieldsAssembler.endRecord();
		} else {
			throw new IncompatibleSchemaException(String.format("Unexpected type %s", flinkType.toString()), null);
		}

		if (nullable) {
			return Schema.createUnion(schema, NULL_SCHEMA);
		} else {
			return schema;
		}
	}

	public static SchemaInfo emptySchemaInfo() {
		return SchemaInfo.builder()
			.name("empty")
			.type(SchemaType.NONE)
			.schema(new byte[0])
			.build();
	}

	private static Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

	private static int[] minBytesForPrecision = new int[39];

	static {
		for (int i = 0; i < minBytesForPrecision.length; i++) {
			minBytesForPrecision[i] = computeMinBytesForPrecision(i);
		}
	}

	private static int computeMinBytesForPrecision(int precision) {
		int numBytes = 1;
		while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
			numBytes += 1;
		}
		return numBytes;
	}

	public static class IncompatibleSchemaException extends Exception {
		public IncompatibleSchemaException(String message, Throwable e) {
			super(message, e);
		}

		public IncompatibleSchemaException(String message) {
			this(message, null);
		}
	}
}
