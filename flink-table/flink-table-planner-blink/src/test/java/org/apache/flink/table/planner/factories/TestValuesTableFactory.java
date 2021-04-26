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

package org.apache.flink.table.planner.factories;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.factories.TestValuesRuntimeFunctions.AppendingOutputFormat;
import org.apache.flink.table.planner.factories.TestValuesRuntimeFunctions.AppendingSinkFunction;
import org.apache.flink.table.planner.factories.TestValuesRuntimeFunctions.AsyncTestValueLookupFunction;
import org.apache.flink.table.planner.factories.TestValuesRuntimeFunctions.KeyedUpsertingSinkFunction;
import org.apache.flink.table.planner.factories.TestValuesRuntimeFunctions.RetractingSinkFunction;
import org.apache.flink.table.planner.factories.TestValuesRuntimeFunctions.TestValuesLookupFunction;
import org.apache.flink.table.planner.utils.FilterUtils;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.collection.Seq;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Test implementation of {@link DynamicTableSourceFactory} that creates a source that produces a sequence of values.
 * And {@link TestValuesScanTableSource} can push down filter into table source. And it has some limitations.
 * A predicate can be pushed down only if it satisfies the following conditions:
 * 1. field name is in filterable-fields, which are defined in with properties.
 * 2. the field type all should be comparable.
 * 3. UDF is UPPER or LOWER.
 */
public final class TestValuesTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	// --------------------------------------------------------------------------------------------
	// Data Registration
	// --------------------------------------------------------------------------------------------

	private static final AtomicInteger idCounter = new AtomicInteger(0);
	private static final Map<String, Collection<Row>> registeredData = new HashMap<>();

	/**
	 * Register the given data into the data factory context and return the data id.
	 * The data id can be used as a reference to the registered data in data connector DDL.
	 */
	public static String registerData(Collection<Row> data) {
		String id = String.valueOf(idCounter.incrementAndGet());
		registeredData.put(id, data);
		return id;
	}

	/**
	 * Register the given data into the data factory context and return the data id.
	 * The data id can be used as a reference to the registered data in data connector DDL.
	 */
	public static String registerData(Seq<Row> data) {
		return registerData(JavaScalaConversionUtil.toJava(data));
	}

	/**
	 * Returns received raw results of the registered table sink.
	 * The raw results are encoded with {@link RowKind}.
	 *
	 * @param tableName the table name of the registered table sink.
	 */
	public static List<String> getRawResults(String tableName) {
		return TestValuesRuntimeFunctions.getRawResults(tableName);
	}

	/**
	 * Returns materialized (final) results of the registered table sink.
	 *
	 * @param tableName the table name of the registered table sink.
	 */
	public static List<String> getResults(String tableName) {
		return TestValuesRuntimeFunctions.getResults(tableName);
	}

	/**
	 * Removes the registered data under the given data id.
	 */
	public static void clearAllData() {
		registeredData.clear();
		TestValuesRuntimeFunctions.clearResults();
	}

	/**
	 * Creates a changelog row from the given RowKind short string and value objects.
	 */
	public static Row changelogRow(String rowKind, Object... values) {
		RowKind kind = parseRowKind(rowKind);
		return Row.ofKind(kind, values);
	}

	/**
	 * Parse the given RowKind short string into instance of RowKind.
	 */
	private static RowKind parseRowKind(String rowKindShortString) {
		switch (rowKindShortString) {
			case "+I": return RowKind.INSERT;
			case "-U": return RowKind.UPDATE_BEFORE;
			case "+U": return RowKind.UPDATE_AFTER;
			case "-D": return RowKind.DELETE;
			default: throw new IllegalArgumentException(
				"Unsupported RowKind string: " + rowKindShortString);
		}
	}


	// --------------------------------------------------------------------------------------------
	// Factory
	// --------------------------------------------------------------------------------------------

	public static final AtomicInteger RESOURCE_COUNTER = new AtomicInteger();

	private static final String IDENTIFIER = "values";

	private static final ConfigOption<String> DATA_ID = ConfigOptions
		.key("data-id")
		.stringType()
		.noDefaultValue();

	private static final ConfigOption<Boolean> BOUNDED = ConfigOptions
		.key("bounded")
		.booleanType()
		.defaultValue(false);

	private static final ConfigOption<String> CHANGELOG_MODE = ConfigOptions
		.key("changelog-mode")
		.stringType()
		.defaultValue("I"); // all available "I,UA,UB,D"

	private static final ConfigOption<String> RUNTIME_SOURCE = ConfigOptions
		.key("runtime-source")
		.stringType()
		.defaultValue("SourceFunction"); // another is "InputFormat"

	private static final ConfigOption<String> RUNTIME_SINK = ConfigOptions
		.key("runtime-sink")
		.stringType()
		.defaultValue("SinkFunction"); // another is "OutputFormat"

	private static final ConfigOption<String> TABLE_SOURCE_CLASS = ConfigOptions
		.key("table-source-class")
		.stringType()
		.defaultValue("DEFAULT"); // class path which implements DynamicTableSource

	private static final ConfigOption<String> TABLE_SINK_CLASS = ConfigOptions
		.key("table-sink-class")
		.stringType()
		.defaultValue("DEFAULT"); // class path which implements DynamicTableSink

	private static final ConfigOption<String> LOOKUP_FUNCTION_CLASS  = ConfigOptions
		.key("lookup-function-class")
		.stringType()
		.noDefaultValue();

	private static final ConfigOption<Boolean> ASYNC_ENABLED = ConfigOptions
		.key("async")
		.booleanType()
		.defaultValue(false);

	private static final ConfigOption<Boolean> DISABLE_LOOKUP = ConfigOptions
		.key("disable-lookup")
		.booleanType()
		.defaultValue(false);

	private static final ConfigOption<Boolean> SINK_INSERT_ONLY = ConfigOptions
		.key("sink-insert-only")
		.booleanType()
		.defaultValue(true);

	private static final ConfigOption<Integer> SINK_EXPECTED_MESSAGES_NUM = ConfigOptions
		.key("sink-expected-messages-num")
		.intType()
		.defaultValue(-1);

	private static final ConfigOption<Boolean> NESTED_PROJECTION_SUPPORTED = ConfigOptions
		.key("nested-projection-supported")
		.booleanType()
		.defaultValue(false);

	private static final ConfigOption<List<String>> FILTERABLE_FIELDS = ConfigOptions
		.key("filterable-fields")
		.stringType()
		.asList()
		.noDefaultValue();

	private static final ConfigOption<Map<String, String>> READABLE_METADATA = ConfigOptions
		.key("readable-metadata")
		.mapType()
		.defaultValue(Collections.emptyMap())
		.withDescription(
			"Optional map of 'metadata_key:data_type,...'. The order will be alphabetically. " +
			"The metadata is part of the data when enabled.");

	private static final ConfigOption<Map<String, String>> WRITABLE_METADATA = ConfigOptions
		.key("writable-metadata")
		.mapType()
		.defaultValue(Collections.emptyMap())
		.withDescription(
			"Optional map of 'metadata_key:data_type'. The order will be alphabetically. " +
			"The metadata is part of the data when enabled.");

	/**
	 * Parse partition list from Options with the format as "key1:val1,key2:val2;key1:val3,key2:val4".
	 */
	private static final ConfigOption<List<String>> PARTITION_LIST = ConfigOptions
		.key("partition-list")
		.stringType()
		.asList()
		.defaultValues();

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		helper.validate();

		ChangelogMode changelogMode = parseChangelogMode(helper.getOptions().get(CHANGELOG_MODE));
		String runtimeSource = helper.getOptions().get(RUNTIME_SOURCE);
		boolean isBounded = helper.getOptions().get(BOUNDED);
		String dataId = helper.getOptions().get(DATA_ID);
		String sourceClass = helper.getOptions().get(TABLE_SOURCE_CLASS);
		boolean isAsync = helper.getOptions().get(ASYNC_ENABLED);
		String lookupFunctionClass = helper.getOptions().get(LOOKUP_FUNCTION_CLASS);
		boolean disableLookup = helper.getOptions().get(DISABLE_LOOKUP);
		boolean nestedProjectionSupported = helper.getOptions().get(NESTED_PROJECTION_SUPPORTED);
		Optional<List<String>> filterableFields = helper.getOptions().getOptional(FILTERABLE_FIELDS);
		Set<String> filterableFieldsSet = new HashSet<>();
		filterableFields.ifPresent(filterableFieldsSet::addAll);

		final Map<String, DataType> readableMetadata = convertToMetadataMap(
			helper.getOptions().get(READABLE_METADATA),
			context.getClassLoader());

		if (sourceClass.equals("DEFAULT")) {
			Collection<Row> data = registeredData.getOrDefault(dataId, Collections.emptyList());
			List<Map<String, String>> partitions = parsePartitionList(helper.getOptions().get(PARTITION_LIST));
			DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
			// pushing project into scan will prune schema and we have to get the mapping between partition and row
			Map<Map<String, String>, Collection<Row>> partition2Rows;
			if (!partitions.isEmpty()) {
				partition2Rows = mapPartitionToRow(producedDataType, data, partitions);
			} else {
				// put all data into one partition
				partitions = Collections.emptyList();
				partition2Rows = new HashMap<>();
				partition2Rows.put(Collections.emptyMap(), data);
			}

			if (disableLookup) {
				return new TestValuesScanTableSource(
					producedDataType,
					changelogMode,
					isBounded,
					runtimeSource,
					partition2Rows,
					nestedProjectionSupported,
					null,
					Collections.emptyList(),
					filterableFieldsSet,
					Long.MAX_VALUE,
					partitions,
					readableMetadata,
					null);
			} else {
				return new TestValuesScanLookupTableSource(
					producedDataType,
					changelogMode,
					isBounded,
					runtimeSource,
					partition2Rows,
					isAsync,
					lookupFunctionClass,
					nestedProjectionSupported,
					null,
					Collections.emptyList(),
					filterableFieldsSet,
					Long.MAX_VALUE,
					partitions,
					readableMetadata,
					null);
			}
		} else {
			try {
				return InstantiationUtil.instantiate(
					sourceClass,
					DynamicTableSource.class,
					Thread.currentThread().getContextClassLoader());
			} catch (FlinkException e) {
				throw new TableException("Can't instantiate class " + sourceClass, e);
			}
		}
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		helper.validate();

		String sinkClass = helper.getOptions().get(TABLE_SINK_CLASS);
		boolean isInsertOnly = helper.getOptions().get(SINK_INSERT_ONLY);
		String runtimeSink = helper.getOptions().get(RUNTIME_SINK);
		int expectedNum = helper.getOptions().get(SINK_EXPECTED_MESSAGES_NUM);
		final Map<String, DataType> writableMetadata = convertToMetadataMap(
			helper.getOptions().get(WRITABLE_METADATA),
			context.getClassLoader());

		final DataType consumedType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

		final int[] primaryKeyIndices = TableSchemaUtils.getPrimaryKeyIndices(context.getCatalogTable().getSchema());

		if (sinkClass.equals("DEFAULT")) {
			return new TestValuesTableSink(
				consumedType,
				primaryKeyIndices,
				context.getObjectIdentifier().getObjectName(),
				isInsertOnly,
				runtimeSink,
				expectedNum,
				writableMetadata);
		} else {
			try {
				return InstantiationUtil.instantiate(
					sinkClass,
					DynamicTableSink.class,
					Thread.currentThread().getContextClassLoader());
			} catch (FlinkException e) {
				throw new TableException("Can't instantiate class " + sinkClass, e);
			}
		}
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.emptySet();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return new HashSet<>(Arrays.asList(
			DATA_ID,
			CHANGELOG_MODE,
			BOUNDED,
			RUNTIME_SOURCE,
			TABLE_SOURCE_CLASS,
			LOOKUP_FUNCTION_CLASS,
			ASYNC_ENABLED,
			DISABLE_LOOKUP,
			TABLE_SOURCE_CLASS,
			TABLE_SINK_CLASS,
			SINK_INSERT_ONLY,
			RUNTIME_SINK,
			SINK_EXPECTED_MESSAGES_NUM,
			NESTED_PROJECTION_SUPPORTED,
			FILTERABLE_FIELDS,
			PARTITION_LIST,
			READABLE_METADATA,
			WRITABLE_METADATA));
	}

	private static List<Map<String, String>> parsePartitionList(List<String> stringPartitions) {
		return stringPartitions.stream().map(
			partition -> {
				Map<String, String> spec = new HashMap<>();
				Arrays.stream(partition.split(",")).forEach(pair -> {
					String[] split = pair.split(":");
					spec.put(split[0].trim(), split[1].trim());
				});
				return spec;
			}).collect(Collectors.toList());
	}

	private static Map<Map<String, String>, Collection<Row>> mapPartitionToRow(
			DataType producedDataType,
			Collection<Row> rows,
			List<Map<String, String>> partitions) {
		Map<Map<String, String>, Collection<Row>> map = new HashMap<>();
		for (Map<String, String> partition: partitions) {
			map.put(partition, new ArrayList<>());
		}
		List<String> fieldNames = DataTypeUtils.flattenToNames(producedDataType);
		for (Row row: rows) {
			for (Map<String, String> partition: partitions) {
				boolean match = true;
				for (Map.Entry<String, String> entry: partition.entrySet()) {
					int index = fieldNames.indexOf(entry.getKey());
					if (index < 0) {
						throw new IllegalArgumentException(
							String.format("Illegal partition list: partition key %s is not found in schema.", entry.getKey()));
					}
					if (entry.getValue() != null) {
						if (row.getField(index) == null) {
							match = false;
						} else {
							match = entry.getValue().equals(Objects.requireNonNull(row.getField(index)).toString());
						}
					} else {
						match = row.getField(index) == null;
					}
					if (!match) {
						break;
					}
				}
				if (match) {
					map.get(partition).add(row);
					break;
				}
			}
		}
		return map;
	}

	private ChangelogMode parseChangelogMode(String string) {
		ChangelogMode.Builder builder = ChangelogMode.newBuilder();
		for (String split : string.split(",")) {
			switch (split.trim()) {
				case "I":
					builder.addContainedKind(RowKind.INSERT);
					break;
				case "UB":
					builder.addContainedKind(RowKind.UPDATE_BEFORE);
					break;
				case "UA":
					builder.addContainedKind(RowKind.UPDATE_AFTER);
					break;
				case "D":
					builder.addContainedKind(RowKind.DELETE);
					break;
				default:
					throw new IllegalArgumentException("Invalid ChangelogMode string: " + string);
			}
		}
		return builder.build();
	}

	private static Map<String, DataType> convertToMetadataMap(
			Map<String, String> metadataOption,
			ClassLoader classLoader) {
		return metadataOption.keySet()
			.stream()
			.sorted()
			.collect(
				Collectors.toMap(
					Function.identity(),
					key -> {
						final String typeString = metadataOption.get(key);
						final LogicalType type = LogicalTypeParser.parse(typeString, classLoader);
						return TypeConversions.fromLogicalToDataType(type);
					},
					(u, v) -> {
						throw new IllegalStateException();
					},
					LinkedHashMap::new
				)
			);
	}

	// --------------------------------------------------------------------------------------------
	// Table sources
	// --------------------------------------------------------------------------------------------

	/**
	 * Values {@link ScanTableSource} for testing.
	 */
	private static class TestValuesScanTableSource implements
			ScanTableSource,
			SupportsProjectionPushDown,
			SupportsFilterPushDown,
			SupportsLimitPushDown,
			SupportsPartitionPushDown,
			SupportsReadingMetadata {

		private DataType producedDataType;
		private final ChangelogMode changelogMode;
		private final boolean bounded;
		private final String runtimeSource;
		protected Map<Map<String, String>, Collection<Row>> data;

		private final boolean nestedProjectionSupported;
		private @Nullable int[] projectedPhysicalFields;
		private List<ResolvedExpression> filterPredicates;
		private final Set<String> filterableFields;
		private long limit;
		protected List<Map<String, String>> allPartitions;
		private final Map<String, DataType> readableMetadata;
		private @Nullable int[] projectedMetadataFields;

		private TestValuesScanTableSource(
				DataType producedDataType,
				ChangelogMode changelogMode,
				boolean bounded,
				String runtimeSource,
				Map<Map<String, String>, Collection<Row>> data,
				boolean nestedProjectionSupported,
				@Nullable int[] projectedPhysicalFields,
				List<ResolvedExpression> filterPredicates,
				Set<String> filterableFields,
				long limit,
				List<Map<String, String>> allPartitions,
				Map<String, DataType> readableMetadata,
				@Nullable int[] projectedMetadataFields) {
			this.producedDataType = producedDataType;
			this.changelogMode = changelogMode;
			this.bounded = bounded;
			this.runtimeSource = runtimeSource;
			this.data = data;
			this.nestedProjectionSupported = nestedProjectionSupported;
			this.projectedPhysicalFields = projectedPhysicalFields;
			this.filterPredicates = filterPredicates;
			this.filterableFields = filterableFields;
			this.limit = limit;
			this.allPartitions = allPartitions;
			this.readableMetadata = readableMetadata;
			this.projectedMetadataFields = projectedMetadataFields;
		}

		@Override
		public ChangelogMode getChangelogMode() {
			return changelogMode;
		}

		@Override
		public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
			TypeSerializer<RowData> serializer = runtimeProviderContext
				.<RowData>createTypeInformation(producedDataType)
				.createSerializer(new ExecutionConfig());
			DataStructureConverter converter = runtimeProviderContext.createDataStructureConverter(producedDataType);
			converter.open(RuntimeConverter.Context.create(TestValuesTableFactory.class.getClassLoader()));
			Collection<RowData> values = convertToRowData(converter);

			if (runtimeSource.equals("SourceFunction")) {
				try {
					return SourceFunctionProvider.of(
						new FromElementsFunction<>(serializer, values),
						bounded);
				} catch (IOException e) {
					throw new TableException("Fail to init source function", e);
				}
			} else if (runtimeSource.equals("InputFormat")) {
				return InputFormatProvider.of(new CollectionInputFormat<>(values, serializer));
			} else {
				throw new IllegalArgumentException("Unsupported runtime source class: " + runtimeSource);
			}
		}

		@Override
		public boolean supportsNestedProjection() {
			return nestedProjectionSupported;
		}

		@Override
		public void applyProjection(int[][] projectedFields) {
			this.producedDataType = DataTypeUtils.projectRow(producedDataType, projectedFields);
			this.projectedPhysicalFields = Arrays.stream(projectedFields).mapToInt(f -> f[0]).toArray();
		}

		@Override
		public Result applyFilters(List<ResolvedExpression> filters) {
			List<ResolvedExpression> acceptedFilters = new ArrayList<>();
			List<ResolvedExpression> remainingFilters = new ArrayList<>();
			for (ResolvedExpression expr : filters) {
				if (FilterUtils.shouldPushDown(expr, filterableFields)) {
					acceptedFilters.add(expr);
				} else {
					remainingFilters.add(expr);
				}
			}
			this.filterPredicates = acceptedFilters;
			return Result.of(acceptedFilters, remainingFilters);
		}

		private Function<String, Comparable<?>> getValueGetter(Row row) {
			final List<String> fieldNames = DataTypeUtils.flattenToNames(producedDataType);
			return fieldName -> {
				int idx = fieldNames.indexOf(fieldName);
				return (Comparable<?>) row.getField(idx);
			};
		}

		@Override
		public DynamicTableSource copy() {
			return new TestValuesScanTableSource(
				producedDataType,
				changelogMode,
				bounded,
				runtimeSource,
				data,
				nestedProjectionSupported,
				projectedPhysicalFields,
				filterPredicates,
				filterableFields,
				limit,
				allPartitions,
				readableMetadata,
				projectedMetadataFields);
		}

		@Override
		public String asSummaryString() {
			return "TestValues";
		}

		private Collection<RowData> convertToRowData(DataStructureConverter converter) {
			List<RowData> result = new ArrayList<>();
			List<Map<String, String>> keys = allPartitions.isEmpty() ?
				Collections.singletonList(Collections.emptyMap()) :
				allPartitions;
			for (Map<String, String> partition: keys) {
				for (Row row : data.get(partition)) {
					if (result.size() >= limit) {
						return result;
					}
					boolean isRetained = FilterUtils.isRetainedAfterApplyingFilterPredicates(
						filterPredicates,
						getValueGetter(row));
					if (isRetained) {
						final Row projectedRow = projectRow(row);
						final RowData rowData = (RowData) converter.toInternal(projectedRow);
						if (rowData != null) {
							rowData.setRowKind(row.getKind());
							result.add(rowData);
						}
					}
				}
			}
			return result;
		}

		private Row projectRow(Row row) {
			if (projectedPhysicalFields == null) {
				return row;
			}

			final IntStream projectedPhysicalStream = IntStream.of(projectedPhysicalFields);
			final IntStream projectedMetadataStream = (projectedMetadataFields != null) ?
				IntStream.of(projectedMetadataFields).map(i -> i + projectedPhysicalFields.length) :
				IntStream.empty();
			final int[] projectedFields = IntStream
				.concat(
					projectedPhysicalStream,
					projectedMetadataStream)
				.toArray();
			return Row.project(row, projectedFields);
		}

		@Override
		public Optional<List<Map<String, String>>> listPartitions() {
			if (allPartitions.isEmpty()) {
				throw new UnsupportedOperationException("Please use catalog to read partitions");
			}
			return Optional.of(allPartitions);
		}

		@Override
		public void applyPartitions(List<Map<String, String>> remainingPartitions) {
			// remainingPartition is non-nullable.
			if (allPartitions.isEmpty()) {
				// read partitions from catalog
				if (!remainingPartitions.isEmpty()) {
					// map data into partitions
					this.allPartitions = remainingPartitions;
					this.data = mapPartitionToRow(producedDataType, data.get(Collections.EMPTY_MAP), remainingPartitions);
				} else {
					// we will read data from Collections.emptyList() if allPartitions is empty.
					// therefore, we should clear all data manually.
					this.data.put(Collections.emptyMap(), Collections.emptyList());
				}
			} else {
				this.allPartitions = remainingPartitions;
				if (remainingPartitions.isEmpty()) {
					this.data.put(Collections.emptyMap(), Collections.emptyList());
				}
			}
		}

		@Override
		public void applyLimit(long limit) {
			this.limit = limit;
		}

		@Override
		public Map<String, DataType> listReadableMetadata() {
			return readableMetadata;
		}

		@Override
		public void applyReadableMetadata(List<String> remainingMetadataKeys, DataType newProducedDataType) {
			producedDataType = newProducedDataType;
			final List<String> allMetadataKeys = new ArrayList<>(listReadableMetadata().keySet());
			projectedMetadataFields = remainingMetadataKeys.stream()
				.mapToInt(allMetadataKeys::indexOf)
				.toArray();
		}
	}

	/**
	 * Values {@link LookupTableSource} and {@link ScanTableSource} for testing.
	 *
	 * <p>Note: we separate the implementations for scan and lookup to make it possible to support
	 * a scan source without lookup ability, e.g. testing temporal join changelog source.
	 */
	private static class TestValuesScanLookupTableSource
		extends TestValuesScanTableSource
		implements LookupTableSource {

		private final @Nullable String lookupFunctionClass;
		private final boolean isAsync;

		private TestValuesScanLookupTableSource(
				DataType producedDataType,
				ChangelogMode changelogMode,
				boolean bounded,
				String runtimeSource,
				Map<Map<String, String>, Collection<Row>> data,
				boolean isAsync,
				@Nullable String lookupFunctionClass,
				boolean nestedProjectionSupported,
				int[] projectedFields,
				List<ResolvedExpression> filterPredicates,
				Set<String> filterableFields,
				long limit,
				List<Map<String, String>> allPartitions,
				Map<String, DataType> readableMetadata,
				@Nullable int[] projectedMetadataFields) {
			super(
				producedDataType,
				changelogMode,
				bounded,
				runtimeSource,
				data,
				nestedProjectionSupported,
				projectedFields,
				filterPredicates,
				filterableFields,
				limit,
				allPartitions,
				readableMetadata,
				projectedMetadataFields);
			this.lookupFunctionClass = lookupFunctionClass;
			this.isAsync = isAsync;
		}

		@SuppressWarnings({"unchecked", "rawtypes"})
		@Override
		public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
			if (lookupFunctionClass != null) {
				// use the specified lookup function
				try {
					Class<?> clazz = Class.forName(lookupFunctionClass);
					Object udtf = InstantiationUtil.instantiate(clazz);
					if (udtf instanceof TableFunction) {
						return TableFunctionProvider.of((TableFunction) udtf);
					} else {
						return AsyncTableFunctionProvider.of((AsyncTableFunction) udtf);
					}
				} catch (ClassNotFoundException e) {
					throw new IllegalArgumentException("Could not instantiate class: " + lookupFunctionClass);
				}
			}

			int[] lookupIndices = Arrays.stream(context.getKeys())
				.mapToInt(k -> k[0])
				.toArray();
			Map<Row, List<Row>> mapping = new HashMap<>();
			Collection<Row> rows;
			if (allPartitions.equals(Collections.EMPTY_LIST)) {
				rows = data.getOrDefault(Collections.EMPTY_MAP, Collections.EMPTY_LIST);
			} else {
				rows = new ArrayList<>();
				allPartitions.forEach(key -> rows.addAll(data.getOrDefault(key, new ArrayList<>())));
			}
			rows.forEach(record -> {
				Row key = Row.of(Arrays.stream(lookupIndices)
					.mapToObj(record::getField)
					.toArray());
				List<Row> list = mapping.get(key);
				if (list != null) {
					list.add(record);
				} else {
					list = new ArrayList<>();
					list.add(record);
					mapping.put(key, list);
				}
			});
			if (isAsync) {
				return AsyncTableFunctionProvider.of(new AsyncTestValueLookupFunction(mapping));
			} else {
				return TableFunctionProvider.of(new TestValuesLookupFunction(mapping));
			}
		}
	}

	/**
	 * A mocked {@link LookupTableSource} for validation test.
	 */
	public static class MockedLookupTableSource implements LookupTableSource {

		@Override
		public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
			return null;
		}

		@Override
		public DynamicTableSource copy() {
			return null;
		}

		@Override
		public String asSummaryString() {
			return null;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Table sinks
	// --------------------------------------------------------------------------------------------

	/**
	 * Values {@link DynamicTableSink} for testing.
	 */
	private static class TestValuesTableSink implements
			DynamicTableSink,
			SupportsWritingMetadata {

		private DataType consumedDataType;
		private int[] primaryKeyIndices;
		private final String tableName;
		private final boolean isInsertOnly;
		private final String runtimeSink;
		private final int expectedNum;
		private final Map<String, DataType> writableMetadata;

		private TestValuesTableSink(
				DataType consumedDataType,
				int[] primaryKeyIndices,
				String tableName,
				boolean isInsertOnly,
				String runtimeSink,
				int expectedNum,
				Map<String, DataType> writableMetadata) {
			this.consumedDataType = consumedDataType;
			this.primaryKeyIndices = primaryKeyIndices;
			this.tableName = tableName;
			this.isInsertOnly = isInsertOnly;
			this.runtimeSink = runtimeSink;
			this.expectedNum = expectedNum;
			this.writableMetadata = writableMetadata;
		}

		@Override
		public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
			if (isInsertOnly) {
				return ChangelogMode.insertOnly();
			} else {
				ChangelogMode.Builder builder = ChangelogMode.newBuilder();
				if (primaryKeyIndices.length > 0) {
					// can update on key, ignore UPDATE_BEFORE
					for (RowKind kind : requestedMode.getContainedKinds()) {
						if (kind != RowKind.UPDATE_BEFORE) {
							builder.addContainedKind(kind);
						}
					}
					return builder.build();
				} else {
					// don't have key, works in retract mode
					return requestedMode;
				}
			}
		}

		@Override
		public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
			DataStructureConverter converter = context.createDataStructureConverter(consumedDataType);
			if (isInsertOnly) {
				checkArgument(expectedNum == -1,
					"Appending Sink doesn't support '" + SINK_EXPECTED_MESSAGES_NUM.key() + "' yet.");
				if (runtimeSink.equals("SinkFunction")) {
					return SinkFunctionProvider.of(
						new AppendingSinkFunction(
							tableName,
							converter));
				} else if (runtimeSink.equals("OutputFormat")) {
					return OutputFormatProvider.of(
						new AppendingOutputFormat(
							tableName,
							converter));
				} else {
					throw new IllegalArgumentException("Unsupported runtime sink class: " + runtimeSink);
				}
			} else {
				// we don't support OutputFormat for updating query in the TestValues connector
				assert runtimeSink.equals("SinkFunction");
				SinkFunction<RowData> sinkFunction;
				if (primaryKeyIndices.length > 0) {
					sinkFunction = new KeyedUpsertingSinkFunction(
						tableName,
						converter,
						primaryKeyIndices,
						expectedNum);
				} else {
					checkArgument(expectedNum == -1,
						"Retracting Sink doesn't support '" + SINK_EXPECTED_MESSAGES_NUM.key() + "' yet.");
					sinkFunction = new RetractingSinkFunction(
						tableName,
						converter);
				}
				return SinkFunctionProvider.of(sinkFunction);
			}
		}

		@Override
		public DynamicTableSink copy() {
			return new TestValuesTableSink(
				consumedDataType,
				primaryKeyIndices,
				tableName,
				isInsertOnly,
				runtimeSink,
				expectedNum,
				writableMetadata);
		}

		@Override
		public String asSummaryString() {
			return "TestValues";
		}

		@Override
		public Map<String, DataType> listWritableMetadata() {
			return writableMetadata;
		}

		@Override
		public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
			this.consumedDataType = consumedDataType;
		}
	}

	/**
	 * A TableSink used for testing the implementation of {@link SinkFunction.Context}.
	 */
	public static class TestSinkContextTableSink implements DynamicTableSink {

		public static final List<Long> ROWTIMES = new ArrayList<>();

		@Override
		public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
			return ChangelogMode.insertOnly();
		}

		@Override
		public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
			// clear ROWTIMES first
			synchronized (ROWTIMES) {
				ROWTIMES.clear();
			}
			SinkFunction<RowData> sinkFunction = new SinkFunction<RowData>() {
				private static final long serialVersionUID = -4871941979714977824L;
				@Override
				public void invoke(RowData value, Context context) throws Exception {
					synchronized (ROWTIMES) {
						ROWTIMES.add(context.timestamp());
					}
				}
			};
			return SinkFunctionProvider.of(sinkFunction);
		}

		@Override
		public DynamicTableSink copy() {
			return new TestSinkContextTableSink();
		}

		@Override
		public String asSummaryString() {
			return "TestSinkContextTableSink";
		}
	}
}
