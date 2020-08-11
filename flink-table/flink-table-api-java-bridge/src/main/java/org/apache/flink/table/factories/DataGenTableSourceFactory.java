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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.datagen.DataGenTableSource;
import org.apache.flink.table.factories.datagen.DataGeneratorContainer;
import org.apache.flink.table.factories.datagen.RandomGeneratorVisitor;
import org.apache.flink.table.factories.datagen.SequenceGeneratorVisitor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * Factory for creating configured instances of {@link DataGenTableSource} in a stream environment.
 */
@PublicEvolving
public class DataGenTableSourceFactory implements DynamicTableSourceFactory {

	public static final String IDENTIFIER = "datagen";
	public static final Long ROWS_PER_SECOND_DEFAULT_VALUE = 10000L;

	public static final ConfigOption<Long> ROWS_PER_SECOND = key("rows-per-second")
			.longType()
			.defaultValue(ROWS_PER_SECOND_DEFAULT_VALUE)
			.withDescription("Rows per second to control the emit rate.");

	public static final ConfigOption<Long> NUMBER_OF_ROWS = key("number-of-rows")
		.longType()
		.noDefaultValue()
		.withDescription("Total number of rows to emit. By default, the source is unbounded.");

	public static final String FIELDS = "fields";
	public static final String KIND = "kind";
	public static final String START = "start";
	public static final String END = "end";
	public static final String MIN = "min";
	public static final String MAX = "max";
	public static final String LENGTH = "length";

	public static final String SEQUENCE = "sequence";
	public static final String RANDOM = "random";

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return new HashSet<>();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(ROWS_PER_SECOND);
		options.add(NUMBER_OF_ROWS);
		return options;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		Configuration options = new Configuration();
		context.getCatalogTable().getOptions().forEach(options::setString);

		TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		DataGenerator<?>[] fieldGenerators = new DataGenerator[schema.getFieldCount()];
		Set<ConfigOption<?>> optionalOptions = new HashSet<>();

		for (int i = 0; i < fieldGenerators.length; i++) {
			String name = schema.getFieldNames()[i];
			DataType type = schema.getFieldDataTypes()[i];

			ConfigOption<String> kind = key(FIELDS + "." + name + "." + KIND)
					.stringType().defaultValue(RANDOM);
			DataGeneratorContainer container = createContainer(name, type, options.get(kind), options);
			fieldGenerators[i] = container.getGenerator();

			optionalOptions.add(kind);
			optionalOptions.addAll(container.getOptions());
		}

		FactoryUtil.validateFactoryOptions(requiredOptions(), optionalOptions, options);

		Set<String> consumedOptionKeys = new HashSet<>();
		consumedOptionKeys.add(CONNECTOR.key());
		consumedOptionKeys.add(ROWS_PER_SECOND.key());
		consumedOptionKeys.add(NUMBER_OF_ROWS.key());
		optionalOptions.stream().map(ConfigOption::key).forEach(consumedOptionKeys::add);
		FactoryUtil.validateUnconsumedKeys(factoryIdentifier(), options.keySet(), consumedOptionKeys);

		String name = context.getObjectIdentifier().toString();
		return new DataGenTableSource(fieldGenerators, name, schema, options.get(ROWS_PER_SECOND), options.get(NUMBER_OF_ROWS));
	}

	private DataGeneratorContainer createContainer(
			String name, DataType type, String kind, ReadableConfig options) {
		switch (kind) {
			case RANDOM:
				return type.getLogicalType().accept(new RandomGeneratorVisitor(name, options));
			case SEQUENCE:
				return type.getLogicalType().accept(new SequenceGeneratorVisitor(name, options));
			default:
				throw new ValidationException("Unsupported generator kind: " + kind);
		}
	}
}
