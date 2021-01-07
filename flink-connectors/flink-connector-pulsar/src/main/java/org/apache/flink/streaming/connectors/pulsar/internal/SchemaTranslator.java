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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.BooleanSchema;
import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.DoubleSchema;
import org.apache.pulsar.client.impl.schema.FloatSchema;
import org.apache.pulsar.client.impl.schema.IntSchema;
import org.apache.pulsar.client.impl.schema.LocalDateSchema;
import org.apache.pulsar.client.impl.schema.LocalDateTimeSchema;
import org.apache.pulsar.client.impl.schema.LocalTimeSchema;
import org.apache.pulsar.client.impl.schema.LongSchema;
import org.apache.pulsar.client.impl.schema.ShortSchema;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.io.Serializable;

/**
 * schema translator.
 */
@Internal
public abstract class SchemaTranslator implements Serializable {

	public abstract SchemaInfo tableSchemaToPulsarSchema(TableSchema schema) throws IncompatibleSchemaException;

	public abstract TableSchema pulsarSchemaToTableSchema(SchemaInfo pulsarSchema) throws IncompatibleSchemaException;

	public abstract FieldsDataType pulsarSchemaToFieldsDataType(SchemaInfo pulsarSchema)
		throws IncompatibleSchemaException;

	public abstract DataType schemaInfo2SqlType(SchemaInfo si) throws IncompatibleSchemaException;

	public static Schema atomicType2PulsarSchema(DataType flinkType) throws IncompatibleSchemaException {
		LogicalTypeRoot type = flinkType.getLogicalType().getTypeRoot();
		switch (type) {
			case BOOLEAN:
				return BooleanSchema.of();
			case VARBINARY:
				return BytesSchema.of();
			case DATE:
				return LocalDateSchema.of();
			case TIME_WITHOUT_TIME_ZONE:
				return LocalTimeSchema.of();
			case VARCHAR:
				return Schema.STRING;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return LocalDateTimeSchema.of();
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return Schema.INSTANT;
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
				throw new IncompatibleSchemaException(
					String.format("%s is not supported by Pulsar yet", flinkType.toString()), null);
		}
	}
}
