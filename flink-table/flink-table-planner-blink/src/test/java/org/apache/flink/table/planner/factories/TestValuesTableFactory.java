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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.types.DataType;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import scala.collection.Seq;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Test implementation of {@link DynamicTableSourceFactory} that creates
 * a source that produces a sequence of values.
 */
public class TestValuesTableFactory implements DynamicTableSourceFactory {

	// --------------------------------------------------------------------------------------------
	// Data Registration
	// --------------------------------------------------------------------------------------------

	private static final AtomicInteger idCounter = new AtomicInteger(0);
	private static final Map<String, Collection<Tuple2<RowKind, Row>>> registeredData = new HashMap<>();

	/**
	 * Register the given data into the data factory context and return the data id.
	 * The data id can be used as a reference to the registered data in data connector DDL.
	 */
	public static String registerData(Collection<Row> data) {
		List<Tuple2<RowKind, Row>> dataWithKinds = new ArrayList<>();
		for (Row row : data) {
			dataWithKinds.add(Tuple2.of(RowKind.INSERT, row));
		}
		return registerChangelogData(dataWithKinds);
	}

	/**
	 * Register the given data into the data factory context and return the data id.
	 * The data id can be used as a reference to the registered data in data connector DDL.
	 */
	public static String registerData(Seq<Row> data) {
		return registerData(JavaScalaConversionUtil.toJava(data));
	}

	/**
	 * Register the given data with RowKind into the data factory context and return the data id.
	 * The data id can be used as a reference to the registered data in data connector DDL.
	 * TODO: remove this utility once Row supports RowKind.
	 */
	public static String registerChangelogData(Collection<Tuple2<RowKind, Row>> data) {
		String id = String.valueOf(idCounter.incrementAndGet());
		registeredData.put(id, data);
		return id;
	}

	/**
	 * Register the given data with RowKind into the data factory context and return the data id.
	 * The data id can be used as a reference to the registered data in data connector DDL.
	 * TODO: remove this utility once Row supports RowKind.
	 */
	public static String registerChangelogData(Seq<Tuple2<RowKind, Row>> data) {
		return registerChangelogData(JavaScalaConversionUtil.toJava(data));
	}

	/**
	 * Removes the registered data under the given data id.
	 */
	public static void clearAllRegisteredData() {
		registeredData.clear();
	}

	/**
	 * Creates a changelog row from the given RowKind short string and value objects.
	 */
	public static Tuple2<RowKind, Row> changelogRow(String rowKind, Object... values) {
		RowKind kind = parseRowKind(rowKind);
		Row row = Row.of(values);
		return Tuple2.of(kind, row);
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

	private static final ConfigOption<String> TABLE_SOURCE_CLASS = ConfigOptions
		.key("table-source-class")
		.stringType()
		.defaultValue("DEFAULT"); // class path which implements DynamicTableSource

	private static final ConfigOption<String> LOOKUP_FUNCTION_CLASS  = ConfigOptions
		.key("lookup-function-class")
		.stringType()
		.noDefaultValue();

	private static final ConfigOption<Boolean> ASYNC_ENABLED = ConfigOptions
		.key("async")
		.booleanType()
		.defaultValue(false);

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

		if (sourceClass.equals("DEFAULT")) {
			Collection<Tuple2<RowKind, Row>> data = registeredData.getOrDefault(dataId, Collections.emptyList());
			DataType rowDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
			return new TestValuesTableSource(
				changelogMode,
				isBounded,
				runtimeSource,
				rowDataType,
				data,
				isAsync,
				lookupFunctionClass);
		} else {
			try {
				return InstantiationUtil.instantiate(
					sourceClass,
					DynamicTableSource.class,
					Thread.currentThread().getContextClassLoader());
			} catch (FlinkException e) {
				throw new RuntimeException("Can't instantiate class " + sourceClass, e);
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
			ASYNC_ENABLED));
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

	// --------------------------------------------------------------------------------------------
	// Table source
	// --------------------------------------------------------------------------------------------

	/**
	 * Values {@link DynamicTableSource} for testing.
	 */
	private static class TestValuesTableSource implements ScanTableSource, LookupTableSource {

		private final ChangelogMode changelogMode;
		private final boolean bounded;
		private final String runtimeSource;
		private final DataType physicalRowDataType;
		private final Collection<Tuple2<RowKind, Row>> data;
		private final boolean isAsync;
		private final @Nullable String lookupFunctionClass;

		private TestValuesTableSource(
				ChangelogMode changelogMode,
				boolean bounded, String runtimeSource,
				DataType physicalRowDataType,
				Collection<Tuple2<RowKind, Row>> data,
				boolean isAsync,
				@Nullable String lookupFunctionClass) {
			this.changelogMode = changelogMode;
			this.bounded = bounded;
			this.runtimeSource = runtimeSource;
			this.physicalRowDataType = physicalRowDataType;
			this.data = data;
			this.isAsync = isAsync;
			this.lookupFunctionClass = lookupFunctionClass;
		}

		@Override
		public ChangelogMode getChangelogMode() {
			return changelogMode;
		}

		@SuppressWarnings("unchecked")
		@Override
		public ScanRuntimeProvider getScanRuntimeProvider(ScanTableSource.Context runtimeProviderContext) {
			TypeSerializer<RowData> serializer = (TypeSerializer<RowData>) runtimeProviderContext
				.createTypeInformation(physicalRowDataType)
				.createSerializer(new ExecutionConfig());
			DataStructureConverter converter = runtimeProviderContext.createDataStructureConverter(physicalRowDataType);
			Collection<RowData> values = convertToRowData(data, converter);

			if (runtimeSource.equals("SourceFunction")) {
				try {
					return SourceFunctionProvider.of(
						new FromElementsFunction<>(serializer, values),
						bounded);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else if (runtimeSource.equals("InputFormat")) {
				return InputFormatProvider.of(new CollectionInputFormat<>(values, serializer));
			} else {
				throw new IllegalArgumentException("Unsupported runtime source class: " + runtimeSource);
			}
		}

		@SuppressWarnings({"unchecked", "rawtypes"})
		@Override
		public LookupRuntimeProvider getLookupRuntimeProvider(LookupTableSource.Context context) {
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
			data.forEach(entry -> {
				Row key = Row.of(Arrays.stream(lookupIndices)
					.mapToObj(idx -> entry.f1.getField(idx))
					.toArray());
				List<Row> list = mapping.get(key);
				if (list != null) {
					list.add(entry.f1);
				} else {
					list = new ArrayList<>();
					list.add(entry.f1);
					mapping.put(key, list);
				}
			});
			if (isAsync) {
				return AsyncTableFunctionProvider.of(new AsyncTestValueLookupFunction(mapping));
			} else {
				return TableFunctionProvider.of(new TestValuesLookupFunction(mapping));
			}
		}

		@Override
		public DynamicTableSource copy() {
			return new TestValuesTableSource(changelogMode, bounded, runtimeSource, physicalRowDataType, data, isAsync, lookupFunctionClass);
		}

		@Override
		public String asSummaryString() {
			return "TestValues";
		}

		private static Collection<RowData> convertToRowData(
				Collection<Tuple2<RowKind, Row>> data,
				DataStructureConverter converter) {
			List<RowData> result = new ArrayList<>();
			for (Tuple2<RowKind, Row> value : data) {
				RowData rowData = (RowData) converter.toInternal(value.f1);
				if (rowData != null) {
					rowData.setRowKind(value.f0);
					result.add(rowData);
				}
			}
			return result;
		}
	}

	/**
	 * A lookup function which find matched rows with the given fields.
	 */
	public static class TestValuesLookupFunction extends TableFunction<Row> {

		private static final long serialVersionUID = 1L;
		private final Map<Row, List<Row>> data;
		private transient boolean isOpenCalled = false;

		private TestValuesLookupFunction(Map<Row, List<Row>> data) {
			this.data = data;
		}

		@Override
		public void open(FunctionContext context) throws Exception {
			RESOURCE_COUNTER.incrementAndGet();
			isOpenCalled = true;
		}

		public void eval(Object... inputs) {
			checkArgument(isOpenCalled, "open() is not called.");
			Row key = Row.of(inputs);
			if (Arrays.asList(inputs).contains(null)) {
				throw new IllegalArgumentException(String.format(
					"Lookup key %s contains null value, which should not happen.", key));
			}
			List<Row> list = data.get(key);
			if (list != null) {
				list.forEach(this::collect);
			}
		}

		@Override
		public void close() throws Exception {
			RESOURCE_COUNTER.decrementAndGet();
		}
	}

	/**
	 * An async lookup function which find matched rows with the given fields.
	 */
	public static class AsyncTestValueLookupFunction extends AsyncTableFunction<Row> {

		private static final long serialVersionUID = 1L;
		private final Map<Row, List<Row>> mapping;
		private transient boolean isOpenCalled = false;
		private transient ExecutorService executor;

		private AsyncTestValueLookupFunction(Map<Row, List<Row>> mapping) {
			this.mapping = mapping;
		}

		@Override
		public void open(FunctionContext context) throws Exception {
			RESOURCE_COUNTER.incrementAndGet();
			isOpenCalled = true;
			executor = Executors.newSingleThreadExecutor();
		}

		public void eval(CompletableFuture<Collection<Row>> resultFuture, Object... inputs) {
			checkArgument(isOpenCalled, "open() is not called.");
			final Row key = Row.of(inputs);
			if (Arrays.asList(inputs).contains(null)) {
				throw new IllegalArgumentException(String.format(
					"Lookup key %s contains null value, which should not happen.", key));
			}
			CompletableFuture
				.supplyAsync(() -> {
					List<Row> list = mapping.get(key);
					if (list == null) {
						return Collections.<Row>emptyList();
					} else {
						return list;
					}
				}, executor)
				.thenAccept(resultFuture::complete);
		}

		@Override
		public void close() throws Exception {
			RESOURCE_COUNTER.decrementAndGet();
			if (executor != null && !executor.isShutdown()) {
				executor.shutdown();
			}
		}
	}

	/**
	 * A mocked {@link LookupTableSource} for validation test.
	 */
	public static class MockedLookupTableSource implements LookupTableSource {

		@Override
		public LookupRuntimeProvider getLookupRuntimeProvider(Context context) {
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

	/**
	 * A mocked {@link ScanTableSource} with {@link SupportsFilterPushDown} ability for validation test.
	 */
	public static class MockedFilterPushDownTableSource implements ScanTableSource, SupportsFilterPushDown {

		@Override
		public ChangelogMode getChangelogMode() {
			return ChangelogMode.insertOnly();
		}

		@Override
		public ScanRuntimeProvider getScanRuntimeProvider(Context runtimeProviderContext) {
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

		@Override
		public Result applyFilters(List<ResolvedExpression> filters) {
			return null;
		}
	}
}
