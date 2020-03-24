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

package org.apache.flink.table.sources.datagen;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.TABLE_SCHEMA_EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * Factory for creating configured instances of {@link DataGenTableSource} in a stream environment.
 */
@Experimental
public class DataGenTableSourceFactory implements TableSourceFactory<Row> {

	public static final String CONNECTOR_TYPE_VALUE = "datagen";
	public static final String CONNECTOR_ROWS_PER_SECOND = "connector.rows-per-second";

	public static final String GENERATOR = "generator";
	public static final String START = "start";
	public static final String END = "end";
	public static final String MIN = "min";
	public static final String MAX = "max";
	public static final String LENGTH = "length";

	public static final String SEQUENCE = "sequence";
	public static final String RANDOM = "random";

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		// connector
		properties.add(SCHEMA + ".*");
		properties.add(CONNECTOR_ROWS_PER_SECOND);
		// schema
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + DescriptorProperties.TABLE_SCHEMA_NAME);
		properties.add(SCHEMA + ".#." + TABLE_SCHEMA_EXPR);
		// watermark
		properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_ROWTIME);
		properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_EXPR);
		properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_DATA_TYPE);
		return properties;
	}

	@Override
	public TableSource<Row> createTableSource(Context context) {
		DescriptorProperties params = new DescriptorProperties();
		params.putProperties(context.getTable().getProperties());

		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(context.getTable().getSchema());

		DataGenerator[] fieldGenerators = new DataGenerator[tableSchema.getFieldCount()];
		for (int i = 0; i < fieldGenerators.length; i++) {
			fieldGenerators[i] = createDataGenerator(
					tableSchema.getFieldName(i).get(),
					tableSchema.getFieldDataType(i).get(),
					params);
		}

		return new DataGenTableSource(fieldGenerators, tableSchema, params.getLong(CONNECTOR_ROWS_PER_SECOND));
	}

	private DataGenerator createDataGenerator(String name, DataType type, DescriptorProperties params) {
		String genType = params.getOptionalString(SCHEMA + "." + name + "." + GENERATOR).orElse(RANDOM);
		switch (genType) {
			case RANDOM:
				return createRandomGenerator(name, type, params);
			case SEQUENCE:
				return createSequenceGenerator(name, type, params);
			default:
				throw new ValidationException("Unsupported generator type: " + genType);
		}
	}

	private DataGenerator createRandomGenerator(String name, DataType type, DescriptorProperties params) {
		String lenKey = SCHEMA + "." + name + "." + GENERATOR + "." + LENGTH;
		String minKey = SCHEMA + "." + name + "." + GENERATOR + "." + MIN;
		String maxKey = SCHEMA + "." + name + "." + GENERATOR + "." + MAX;
		switch (type.getLogicalType().getTypeRoot()) {
			case BOOLEAN:
				return RandomGenerator.booleanGenerator();
			case CHAR:
			case VARCHAR:
				int length = params.getOptionalInt(lenKey).orElse(100);
				return RandomGenerator.stringGenerator(length);
			case TINYINT:
				return RandomGenerator.byteGenerator(
						params.getOptionalByte(minKey).orElse(Byte.MIN_VALUE),
						params.getOptionalByte(maxKey).orElse(Byte.MAX_VALUE));
			case SMALLINT:
				return RandomGenerator.shortGenerator(
						params.getOptionalShort(minKey).orElse(Short.MIN_VALUE),
						params.getOptionalShort(maxKey).orElse(Short.MAX_VALUE));
			case INTEGER:
				return RandomGenerator.intGenerator(
						params.getOptionalInt(minKey).orElse(Integer.MIN_VALUE),
						params.getOptionalInt(maxKey).orElse(Integer.MAX_VALUE));
			case BIGINT:
				return RandomGenerator.longGenerator(
						params.getOptionalLong(minKey).orElse(Long.MIN_VALUE),
						params.getOptionalLong(maxKey).orElse(Long.MAX_VALUE));
			case FLOAT:
				return RandomGenerator.floatGenerator(
						params.getOptionalFloat(minKey).orElse(Float.MIN_VALUE),
						params.getOptionalFloat(maxKey).orElse(Float.MAX_VALUE));
			case DOUBLE:
				return RandomGenerator.doubleGenerator(
						params.getOptionalDouble(minKey).orElse(Double.MIN_VALUE),
						params.getOptionalDouble(maxKey).orElse(Double.MAX_VALUE));
			default:
				throw new ValidationException("Unsupported type: " + type);
		}
	}

	private DataGenerator createSequenceGenerator(String name, DataType type, DescriptorProperties params) {
		String startKey = SCHEMA + "." + name + "." + GENERATOR + "." + START;
		String endKey = SCHEMA + "." + name + "." + GENERATOR + "." + END;
		switch (type.getLogicalType().getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return SequenceGenerator.stringGenerator(
						params.getOptionalLong(startKey).orElse(Long.MIN_VALUE),
						params.getOptionalLong(endKey).orElse(Long.MAX_VALUE));
			case TINYINT:
				return SequenceGenerator.byteGenerator(
						params.getOptionalByte(startKey).orElse(Byte.MIN_VALUE),
						params.getOptionalByte(endKey).orElse(Byte.MAX_VALUE));
			case SMALLINT:
				return SequenceGenerator.shortGenerator(
						params.getOptionalShort(startKey).orElse(Short.MIN_VALUE),
						params.getOptionalShort(endKey).orElse(Short.MAX_VALUE));
			case INTEGER:
				return SequenceGenerator.intGenerator(
						params.getOptionalInt(startKey).orElse(Integer.MIN_VALUE),
						params.getOptionalInt(endKey).orElse(Integer.MAX_VALUE));
			case BIGINT:
				return SequenceGenerator.longGenerator(
						params.getOptionalLong(startKey).orElse(Long.MIN_VALUE),
						params.getOptionalLong(endKey).orElse(Long.MAX_VALUE));
			case FLOAT:
				return SequenceGenerator.floatGenerator(
						params.getOptionalShort(startKey).orElse(Short.MIN_VALUE),
						params.getOptionalShort(endKey).orElse(Short.MAX_VALUE));
			case DOUBLE:
				return SequenceGenerator.doubleGenerator(
						params.getOptionalInt(startKey).orElse(Integer.MIN_VALUE),
						params.getOptionalInt(endKey).orElse(Integer.MAX_VALUE));
			default:
				throw new ValidationException("Unsupported type: " + type);
		}
	}
}
