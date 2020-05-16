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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

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
import java.util.concurrent.atomic.AtomicInteger;

import scala.collection.Seq;

/**
 * Test implementation of {@link DynamicTableSourceFactory} that supports projection push down.
 */
public class TestProjectableValuesTableFactory implements DynamicTableSourceFactory {

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
	 * Removes the registered data under the given data id.
	 */
	public static void clearAllRegisteredData() {
		registeredData.clear();
	}

	// --------------------------------------------------------------------------------------------
	// Factory
	// --------------------------------------------------------------------------------------------

	private static final String IDENTIFIER = "projectable-values";

	private static final ConfigOption<String> DATA_ID = ConfigOptions
			.key("data-id")
			.stringType()
			.defaultValue(null);

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

	private static final ConfigOption<Boolean> NESTED_PROJECTION_SUPPORTED = ConfigOptions
			.key("nested-projection-supported")
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
		boolean nestedProjectionSupported = helper.getOptions().get(NESTED_PROJECTION_SUPPORTED);

		Collection<Tuple2<RowKind, Row>> data = registeredData.getOrDefault(dataId, Collections.emptyList());
		DataType rowDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
		return new TestProjectableValuesTableSource(
				changelogMode,
				isBounded,
				runtimeSource,
				rowDataType,
				data,
				nestedProjectionSupported);
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
				NESTED_PROJECTION_SUPPORTED));
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
	private static class TestProjectableValuesTableSource implements ScanTableSource, SupportsProjectionPushDown {

		private final ChangelogMode changelogMode;
		private final boolean bounded;
		private final String runtimeSource;
		private DataType physicalRowDataType;
		private final Collection<Tuple2<RowKind, Row>> data;
		private final boolean nestedProjectionSupported;
		private int[] projectedFields = null;

		private TestProjectableValuesTableSource(
				ChangelogMode changelogMode,
				boolean bounded, String runtimeSource,
				DataType physicalRowDataType,
				Collection<Tuple2<RowKind, Row>> data,
				boolean nestedProjectionSupported) {
			this.changelogMode = changelogMode;
			this.bounded = bounded;
			this.runtimeSource = runtimeSource;
			this.physicalRowDataType = physicalRowDataType;
			this.data = data;
			this.nestedProjectionSupported = nestedProjectionSupported;
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
			Collection<RowData> values = convertToRowData(data, projectedFields, converter);

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

		@Override
		public DynamicTableSource copy() {
			TestProjectableValuesTableSource newTableSource = new TestProjectableValuesTableSource(
					changelogMode, bounded, runtimeSource, physicalRowDataType, data, nestedProjectionSupported);
			newTableSource.projectedFields = projectedFields;
			return newTableSource;
		}

		@Override
		public String asSummaryString() {
			return "TestProjectableValues";
		}

		private static Collection<RowData> convertToRowData(
				Collection<Tuple2<RowKind, Row>> data,
				@Nullable int[] projectedFields,
				DataStructureConverter converter) {
			List<RowData> result = new ArrayList<>();
			for (Tuple2<RowKind, Row> value : data) {
				Row projectedRow;
				if (projectedFields == null) {
					projectedRow = value.f1;
				} else {
					Object[] newValues = new Object[projectedFields.length];
					for (int i = 0; i < projectedFields.length; ++i) {
						newValues[i] = value.f1.getField(projectedFields[i]);
					}
					projectedRow = Row.of(newValues);
				}
				RowData rowData = (RowData) converter.toInternal(projectedRow);
				if (rowData != null) {
					rowData.setRowKind(value.f0);
					result.add(rowData);
				}
			}
			return result;
		}

		@Override
		public boolean supportsNestedProjection() {
			return nestedProjectionSupported;
		}

		@Override
		public void applyProjection(int[][] projectedFields) {
			this.projectedFields = new int[projectedFields.length];
			FieldsDataType dataType = (FieldsDataType) physicalRowDataType;
			RowType rowType = ((RowType) physicalRowDataType.getLogicalType());
			DataTypes.Field[] fields = new DataTypes.Field[projectedFields.length];
			for (int i = 0; i < projectedFields.length; ++i) {
				int[] projection = projectedFields[i];
				Preconditions.checkArgument(projection.length == 1);
				int index = projection[0];
				this.projectedFields[i] = index;
				fields[i] = DataTypes.FIELD(rowType.getFieldNames().get(index), dataType.getChildren().get(index));
			}
			this.physicalRowDataType = DataTypes.ROW(fields);
		}
	}
}
