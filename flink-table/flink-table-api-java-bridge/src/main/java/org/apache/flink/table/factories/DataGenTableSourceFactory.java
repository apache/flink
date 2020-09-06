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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions.OptionBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Arrays;
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
	public static final int RANDOM_STRING_LENGTH_DEFAULT = 100;

	public static final ConfigOption<Long> ROWS_PER_SECOND = key("rows-per-second")
			.longType()
			.defaultValue(ROWS_PER_SECOND_DEFAULT_VALUE)
			.withDescription("Rows per second to control the emit rate.");

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
		return options;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		Configuration options = new Configuration();
		context.getCatalogTable().getOptions().forEach(options::setString);

		TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		DataGenerator[] fieldGenerators = new DataGenerator[schema.getFieldCount()];
		Set<ConfigOption<?>> optionalOptions = new HashSet<>();

		for (int i = 0; i < fieldGenerators.length; i++) {
			String name = schema.getFieldNames()[i];
			DataType type = schema.getFieldDataTypes()[i];

			ConfigOption<String> kind = key(FIELDS + "." + name + "." + KIND)
					.stringType().defaultValue(RANDOM);
			DataGeneratorContainer container = createContainer(name, type, options.get(kind), options);
			fieldGenerators[i] = container.generator;

			optionalOptions.add(kind);
			optionalOptions.addAll(container.options);
		}

		FactoryUtil.validateFactoryOptions(requiredOptions(), optionalOptions, options);

		Set<String> consumedOptionKeys = new HashSet<>();
		consumedOptionKeys.add(CONNECTOR.key());
		consumedOptionKeys.add(ROWS_PER_SECOND.key());
		optionalOptions.stream().map(ConfigOption::key).forEach(consumedOptionKeys::add);
		FactoryUtil.validateUnconsumedKeys(factoryIdentifier(), options.keySet(), consumedOptionKeys);

		return new DataGenTableSource(fieldGenerators, schema, options.get(ROWS_PER_SECOND));
	}

	private DataGeneratorContainer createContainer(
			String name, DataType type, String kind, ReadableConfig options) {
		switch (kind) {
			case RANDOM:
				return createRandomContainer(name, type, options);
			case SEQUENCE:
				return createSequenceContainer(name, type, options);
			default:
				throw new ValidationException("Unsupported generator kind: " + kind);
		}
	}

	private DataGeneratorContainer createRandomContainer(String name, DataType type, ReadableConfig config) {
		OptionBuilder minKey = key(FIELDS + "." + name + "." + MIN);
		OptionBuilder maxKey = key(FIELDS + "." + name + "." + MAX);
		switch (type.getLogicalType().getTypeRoot()) {
			case BOOLEAN: {
				return DataGeneratorContainer.of(RandomGenerator.booleanGenerator());
			}
			case CHAR:
			case VARCHAR: {
				ConfigOption<Integer> lenOption = key(FIELDS + "." + name + "." + LENGTH)
						.intType()
						.defaultValue(RANDOM_STRING_LENGTH_DEFAULT);
				return DataGeneratorContainer.of(getRandomStringGenerator(config.get(lenOption)), lenOption);
			}
			case TINYINT: {
				ConfigOption<Integer> min = minKey.intType().defaultValue((int) Byte.MIN_VALUE);
				ConfigOption<Integer> max = maxKey.intType().defaultValue((int) Byte.MAX_VALUE);
				return DataGeneratorContainer.of(
						RandomGenerator.byteGenerator(
								config.get(min).byteValue(), config.get(max).byteValue()),
						min, max);
			}
			case SMALLINT: {
				ConfigOption<Integer> min = minKey.intType().defaultValue((int) Short.MIN_VALUE);
				ConfigOption<Integer> max = maxKey.intType().defaultValue((int) Short.MAX_VALUE);
				return DataGeneratorContainer.of(
						RandomGenerator.shortGenerator(
								config.get(min).shortValue(),
								config.get(max).shortValue()),
						min, max);
			}
			case INTEGER: {
				ConfigOption<Integer> min = minKey.intType().defaultValue(Integer.MIN_VALUE);
				ConfigOption<Integer> max = maxKey.intType().defaultValue(Integer.MAX_VALUE);
				return DataGeneratorContainer.of(
						RandomGenerator.intGenerator(
								config.get(min), config.get(max)),
						min, max);
			}
			case BIGINT: {
				ConfigOption<Long> min = minKey.longType().defaultValue(Long.MIN_VALUE);
				ConfigOption<Long> max = maxKey.longType().defaultValue(Long.MAX_VALUE);
				return DataGeneratorContainer.of(
						RandomGenerator.longGenerator(
								config.get(min), config.get(max)),
						min, max);
			}
			case FLOAT: {
				ConfigOption<Float> min = minKey.floatType().defaultValue(Float.MIN_VALUE);
				ConfigOption<Float> max = maxKey.floatType().defaultValue(Float.MAX_VALUE);
				return DataGeneratorContainer.of(
						RandomGenerator.floatGenerator(
								config.get(min), config.get(max)),
						min, max);
			}
			case DOUBLE: {
				ConfigOption<Double> min = minKey.doubleType().defaultValue(Double.MIN_VALUE);
				ConfigOption<Double> max = maxKey.doubleType().defaultValue(Double.MAX_VALUE);
				return DataGeneratorContainer.of(
						RandomGenerator.doubleGenerator(
								config.get(min), config.get(max)),
						min, max);
			}
			default:
				throw new ValidationException("Unsupported type: " + type);
		}
	}

	private DataGeneratorContainer createSequenceContainer(String name, DataType type, ReadableConfig config) {
		String startKeyStr = FIELDS + "." + name + "." + START;
		String endKeyStr = FIELDS + "." + name + "." + END;
		OptionBuilder startKey = key(startKeyStr);
		OptionBuilder endKey = key(endKeyStr);

		config.getOptional(startKey.stringType().noDefaultValue()).orElseThrow(
				() -> new ValidationException(
						"Could not find required property '" + startKeyStr + "' for sequence generator."));
		config.getOptional(endKey.stringType().noDefaultValue()).orElseThrow(
				() -> new ValidationException(
						"Could not find required property '" + endKeyStr + "' for sequence generator."));

		ConfigOption<Integer> intStart = startKey.intType().noDefaultValue();
		ConfigOption<Integer> intEnd = endKey.intType().noDefaultValue();
		ConfigOption<Long> longStart = startKey.longType().noDefaultValue();
		ConfigOption<Long> longEnd = endKey.longType().noDefaultValue();
		switch (type.getLogicalType().getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return DataGeneratorContainer.of(
						getSequenceStringGenerator(
								config.get(longStart), config.get(longEnd)),
						longStart, longEnd);
			case TINYINT:
				return DataGeneratorContainer.of(
						SequenceGenerator.byteGenerator(
								config.get(intStart).byteValue(),
								config.get(intEnd).byteValue()),
						intStart, intEnd);
			case SMALLINT:
				return DataGeneratorContainer.of(
						SequenceGenerator.shortGenerator(
								config.get(intStart).shortValue(),
								config.get(intEnd).shortValue()),
						intStart, intEnd);
			case INTEGER:
				return DataGeneratorContainer.of(
						SequenceGenerator.intGenerator(
								config.get(intStart), config.get(intEnd)),
						intStart, intEnd);
			case BIGINT:
				return DataGeneratorContainer.of(
						SequenceGenerator.longGenerator(
								config.get(longStart), config.get(longEnd)),
						longStart, longEnd);
			case FLOAT:
				return DataGeneratorContainer.of(
						SequenceGenerator.floatGenerator(
								config.get(intStart).shortValue(),
								config.get(intEnd).shortValue()),
						intStart, intEnd);
			case DOUBLE:
				return DataGeneratorContainer.of(
						SequenceGenerator.doubleGenerator(
								config.get(intStart), config.get(intEnd)),
						intStart, intEnd);
			default:
				throw new ValidationException("Unsupported type: " + type);
		}
	}

	private static SequenceGenerator<StringData> getSequenceStringGenerator(long start, long end) {
		return new SequenceGenerator<StringData>(start, end) {
			@Override
			public StringData next() {
				return StringData.fromString(valuesToEmit.poll().toString());
			}
		};
	}

	private static RandomGenerator<StringData> getRandomStringGenerator(int length) {
		return new RandomGenerator<StringData>() {
			@Override
			public StringData next() {
				return StringData.fromString(random.nextHexString(length));
			}
		};
	}

	// -------------------------------- Help Classes -------------------------------

	private static class DataGeneratorContainer {

		private DataGenerator generator;

		/**
		 * Generator config options, for validation.
		 */
		private Set<ConfigOption<?>> options;

		private DataGeneratorContainer(DataGenerator generator, Set<ConfigOption<?>> options) {
			this.generator = generator;
			this.options = options;
		}

		private static DataGeneratorContainer of(
				DataGenerator generator, ConfigOption<?>... options) {
			return new DataGeneratorContainer(generator, new HashSet<>(Arrays.asList(options)));
		}
	}

	/**
	 * A {@link StreamTableSource} that emits each number from a given interval exactly once,
	 * possibly in parallel. See {@link StatefulSequenceSource}.
	 */
	static class DataGenTableSource implements ScanTableSource {

		private final DataGenerator[] fieldGenerators;
		private final TableSchema schema;
		private final long rowsPerSecond;

		private DataGenTableSource(DataGenerator[] fieldGenerators, TableSchema schema, long rowsPerSecond) {
			this.fieldGenerators = fieldGenerators;
			this.schema = schema;
			this.rowsPerSecond = rowsPerSecond;
		}

		@Override
		public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
			return SourceFunctionProvider.of(createSource(), false);
		}

		@VisibleForTesting
		DataGeneratorSource<RowData> createSource() {
			return new DataGeneratorSource<>(
					new RowGenerator(fieldGenerators, schema.getFieldNames()),
					rowsPerSecond);
		}

		@Override
		public DynamicTableSource copy() {
			return new DataGenTableSource(fieldGenerators, schema, rowsPerSecond);
		}

		@Override
		public String asSummaryString() {
			return "DataGenTableSource";
		}

		@Override
		public ChangelogMode getChangelogMode() {
			return ChangelogMode.insertOnly();
		}
	}

	private static class RowGenerator implements DataGenerator<RowData> {

		private static final long serialVersionUID = 1L;

		private final DataGenerator[] fieldGenerators;
		private final String[] fieldNames;

		private RowGenerator(DataGenerator[] fieldGenerators, String[] fieldNames) {
			this.fieldGenerators = fieldGenerators;
			this.fieldNames = fieldNames;
		}

		@Override
		public void open(
				String name,
				FunctionInitializationContext context,
				RuntimeContext runtimeContext) throws Exception {
			for (int i = 0; i < fieldGenerators.length; i++) {
				fieldGenerators[i].open(fieldNames[i], context, runtimeContext);
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			for (DataGenerator generator : fieldGenerators) {
				generator.snapshotState(context);
			}
		}

		@Override
		public boolean hasNext() {
			for (DataGenerator generator : fieldGenerators) {
				if (!generator.hasNext()) {
					return false;
				}
			}
			return true;
		}

		@Override
		public RowData next() {
			GenericRowData row = new GenericRowData(fieldNames.length);
			for (int i = 0; i < fieldGenerators.length; i++) {
				row.setField(i, fieldGenerators[i].next());
			}
			return row;
		}
	}
}
