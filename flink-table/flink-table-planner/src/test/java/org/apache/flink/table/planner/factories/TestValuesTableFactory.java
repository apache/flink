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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.factories.sink.TestValuesTableSink;
import org.apache.flink.table.planner.factories.source.TestValuesScanLookupTableSource;
import org.apache.flink.table.planner.factories.source.TestValuesScanTableSource;
import org.apache.flink.table.planner.factories.source.TestValuesScanTableSourceWithInternalData;
import org.apache.flink.table.planner.factories.source.TestValuesScanTableSourceWithWatermarkPushDown;
import org.apache.flink.table.planner.factories.source.TestValuesScanTableSourceWithoutProjectionPushDown;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import scala.collection.Seq;

/**
 * Test implementation of {@link DynamicTableSourceFactory} that creates a source that produces a
 * sequence of values. And {@link TestValuesScanTableSource} can push down filter into table source.
 * And it has some limitations. A predicate can be pushed down only if it satisfies the following
 * conditions: 1. field name is in filterable-fields, which are defined in with properties. 2. the
 * field type all should be comparable. 3. UDF is UPPER or LOWER.
 */
public final class TestValuesTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    // --------------------------------------------------------------------------------------------
    // Data Registration
    // --------------------------------------------------------------------------------------------

    private static final AtomicInteger idCounter = new AtomicInteger(0);
    private static final Map<String, Collection<Row>> registeredData = new HashMap<>();
    private static final Map<String, Collection<RowData>> registeredRowData = new HashMap<>();

    /**
     * Register the given data into the data factory context and return the data id. The data id can
     * be used as a reference to the registered data in data connector DDL.
     */
    public static String registerData(Collection<Row> data) {
        String id = String.valueOf(idCounter.incrementAndGet());
        registeredData.put(id, data);
        return id;
    }

    /**
     * Register the given data into the data factory context and return the data id. The data id can
     * be used as a reference to the registered data in data connector DDL.
     */
    public static String registerData(Seq<Row> data) {
        return registerData(JavaScalaConversionUtil.toJava(data));
    }

    /**
     * Register the given internal RowData into the data factory context and return the data id. The
     * data id can be used as a reference to the registered data in data connector DDL.
     */
    public static String registerRowData(Collection<RowData> data) {
        String id = String.valueOf(idCounter.incrementAndGet());
        registeredRowData.put(id, data);
        return id;
    }

    /**
     * Register the given internal RowData into the data factory context and return the data id. The
     * data id can be used as a reference to the registered data in data connector DDL.
     */
    public static String registerRowData(Seq<RowData> data) {
        return registerRowData(JavaScalaConversionUtil.toJava(data));
    }

    /**
     * Returns received raw results of the registered table sink. The raw results are encoded with
     * {@link RowKind}.
     *
     * @param tableName the table name of the registered table sink.
     */
    public static List<String> getRawResults(String tableName) {
        return TestValuesRuntimeHelper.getRawResults(tableName);
    }

    public static Map<String, Collection<Row>> getRegisteredData() {
        return registeredData;
    }

    public static Map<String, Collection<RowData>> getRegisteredRowData() {
        return registeredRowData;
    }

    /**
     * Returns received row results if there has been exactly one sink, and throws an error
     * otherwise.
     *
     * <p>The raw results are encoded with {@link RowKind}.
     */
    public static List<String> getOnlyRawResults() {
        return TestValuesRuntimeHelper.getOnlyRawResults();
    }

    /**
     * Returns materialized (final) results of the registered table sink.
     *
     * @param tableName the table name of the registered table sink.
     */
    public static List<String> getResults(String tableName) {
        return TestValuesRuntimeHelper.getResults(tableName);
    }

    public static List<Watermark> getWatermarkOutput(String tableName) {
        return TestValuesRuntimeHelper.getWatermarks(tableName);
    }

    /** Removes the registered data under the given data id. */
    public static void clearAllData() {
        registeredData.clear();
        registeredRowData.clear();
        TestValuesRuntimeHelper.clearResults();
    }

    /** Creates a changelog row from the given RowKind short string and value objects. */
    public static Row changelogRow(String rowKind, Object... values) {
        RowKind kind = parseRowKind(rowKind);
        return Row.ofKind(kind, values);
    }

    /** Parse the given RowKind short string into instance of RowKind. */
    private static RowKind parseRowKind(String rowKindShortString) {
        switch (rowKindShortString) {
            case "+I":
                return RowKind.INSERT;
            case "-U":
                return RowKind.UPDATE_BEFORE;
            case "+U":
                return RowKind.UPDATE_AFTER;
            case "-D":
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException(
                        "Unsupported RowKind string: " + rowKindShortString);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Factory
    // --------------------------------------------------------------------------------------------

    public static final AtomicInteger RESOURCE_COUNTER = new AtomicInteger();

    public static final String IDENTIFIER = "values";

    public static final ConfigOption<String> DATA_ID =
            ConfigOptions.key("data-id").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> BOUNDED =
            ConfigOptions.key("bounded").booleanType().defaultValue(false);

    public static final ConfigOption<String> CHANGELOG_MODE =
            ConfigOptions.key("changelog-mode")
                    .stringType()
                    .defaultValue("I"); // all available "I,UA,UB,D"

    public static final ConfigOption<String> RUNTIME_SOURCE =
            ConfigOptions.key("runtime-source")
                    .stringType()
                    .defaultValue("SourceFunction"); // others are "InputFormat" and "Source"

    public static final ConfigOption<Boolean> FAILING_SOURCE =
            ConfigOptions.key("failing-source").booleanType().defaultValue(false);

    public static final ConfigOption<String> RUNTIME_SINK =
            ConfigOptions.key("runtime-sink").stringType().defaultValue("SinkFunction");
    // others are "OutputFormat" and "SinkWithCollectingWatermark"

    public static final ConfigOption<String> TABLE_SOURCE_CLASS =
            ConfigOptions.key("table-source-class")
                    .stringType()
                    .defaultValue("DEFAULT"); // class path which implements DynamicTableSource

    public static final ConfigOption<String> TABLE_SINK_CLASS =
            ConfigOptions.key("table-sink-class")
                    .stringType()
                    .defaultValue("DEFAULT"); // class path which implements DynamicTableSink

    public static final ConfigOption<String> LOOKUP_FUNCTION_CLASS =
            ConfigOptions.key("lookup-function-class").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> ASYNC_ENABLED =
            ConfigOptions.key("async").booleanType().defaultValue(false);

    public static final ConfigOption<Boolean> ENABLE_LOOKUP =
            ConfigOptions.key("enable-lookup").booleanType().defaultValue(false);

    public static final ConfigOption<Boolean> SINK_INSERT_ONLY =
            ConfigOptions.key("sink-insert-only").booleanType().defaultValue(true);

    public static final ConfigOption<Integer> SINK_EXPECTED_MESSAGES_NUM =
            ConfigOptions.key("sink-expected-messages-num").intType().defaultValue(-1);

    private static final ConfigOption<Boolean> ENABLE_PROJECTION_PUSH_DOWN =
            ConfigOptions.key("enable-projection-push-down").booleanType().defaultValue(true);

    private static final ConfigOption<Boolean> NESTED_PROJECTION_SUPPORTED =
            ConfigOptions.key("nested-projection-supported").booleanType().defaultValue(false);

    public static final ConfigOption<List<String>> FILTERABLE_FIELDS =
            ConfigOptions.key("filterable-fields").stringType().asList().noDefaultValue();

    public static final ConfigOption<Boolean> ENABLE_WATERMARK_PUSH_DOWN =
            ConfigOptions.key("enable-watermark-push-down").booleanType().defaultValue(false);

    public static final ConfigOption<Boolean> INTERNAL_DATA =
            ConfigOptions.key("register-internal-data")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "The registered data is internal type data, "
                                    + "which can be collected by the source directly.");

    public static final ConfigOption<Map<String, String>> READABLE_METADATA =
            ConfigOptions.key("readable-metadata")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "Optional map of 'metadata_key:data_type,...'. The order will be alphabetically. "
                                    + "The metadata is part of the data when enabled.");

    public static final ConfigOption<Map<String, String>> WRITABLE_METADATA =
            ConfigOptions.key("writable-metadata")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "Optional map of 'metadata_key:data_type'. The order will be alphabetically. "
                                    + "The metadata is part of the data when enabled.");

    public static final ConfigOption<Boolean> SINK_DROP_LATE_EVENT =
            ConfigOptions.key("sink.drop-late-event")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("Option to determine whether to discard the late event.");
    public static final ConfigOption<Integer> SOURCE_NUM_ELEMENT_TO_SKIP =
            ConfigOptions.key("source.num-element-to-skip")
                    .intType()
                    .defaultValue(-1)
                    .withDeprecatedKeys("Option to define the number of elements to skip.");

    /**
     * Parse partition list from Options with the format as
     * "key1:val1,key2:val2;key1:val3,key2:val4".
     */
    public static final ConfigOption<List<String>> PARTITION_LIST =
            ConfigOptions.key("partition-list").stringType().asList().defaultValues();

    public static final ConfigOption<String> SINK_CHANGELOG_MODE_ENFORCED =
            ConfigOptions.key("sink-changelog-mode-enforced").stringType().noDefaultValue();

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();

        ChangelogMode changelogMode =
                TestValuesFactoryUtils.parseChangelogMode(helper.getOptions().get(CHANGELOG_MODE));
        String runtimeSource = helper.getOptions().get(RUNTIME_SOURCE);
        boolean isBounded = helper.getOptions().get(BOUNDED);
        String dataId = helper.getOptions().get(DATA_ID);
        String sourceClass = helper.getOptions().get(TABLE_SOURCE_CLASS);
        boolean isAsync = helper.getOptions().get(ASYNC_ENABLED);
        String lookupFunctionClass = helper.getOptions().get(LOOKUP_FUNCTION_CLASS);
        boolean enableLookup = helper.getOptions().get(ENABLE_LOOKUP);
        boolean enableProjectionPushDown = helper.getOptions().get(ENABLE_PROJECTION_PUSH_DOWN);
        boolean nestedProjectionSupported = helper.getOptions().get(NESTED_PROJECTION_SUPPORTED);
        boolean enableWatermarkPushDown = helper.getOptions().get(ENABLE_WATERMARK_PUSH_DOWN);
        boolean failingSource = helper.getOptions().get(FAILING_SOURCE);
        int numElementToSkip = helper.getOptions().get(SOURCE_NUM_ELEMENT_TO_SKIP);
        boolean internalData = helper.getOptions().get(INTERNAL_DATA);

        Optional<List<String>> filterableFields =
                helper.getOptions().getOptional(FILTERABLE_FIELDS);
        Set<String> filterableFieldsSet = new HashSet<>();
        filterableFields.ifPresent(filterableFieldsSet::addAll);

        final Map<String, DataType> readableMetadata =
                TestValuesFactoryUtils.convertToMetadataMap(
                        helper.getOptions().get(READABLE_METADATA), context.getClassLoader());

        if (sourceClass.equals("DEFAULT")) {
            if (internalData) {
                return new TestValuesScanTableSourceWithInternalData(dataId, isBounded);
            }

            Collection<Row> data = registeredData.getOrDefault(dataId, Collections.emptyList());
            List<Map<String, String>> partitions =
                    TestValuesFactoryUtils.parsePartitionList(
                            helper.getOptions().get(PARTITION_LIST));
            DataType producedDataType =
                    context.getCatalogTable().getSchema().toPhysicalRowDataType();
            // pushing project into scan will prune schema and we have to get the mapping between
            // partition and row
            Map<Map<String, String>, Collection<Row>> partition2Rows;
            if (!partitions.isEmpty()) {
                partition2Rows =
                        TestValuesFactoryUtils.mapPartitionToRow(
                                producedDataType, data, partitions);
            } else {
                // put all data into one partition
                partitions = Collections.emptyList();
                partition2Rows = new HashMap<>();
                partition2Rows.put(Collections.emptyMap(), data);
            }

            if (!enableProjectionPushDown) {
                return new TestValuesScanTableSourceWithoutProjectionPushDown(
                        producedDataType,
                        changelogMode,
                        isBounded,
                        runtimeSource,
                        failingSource,
                        partition2Rows,
                        nestedProjectionSupported,
                        null,
                        Collections.emptyList(),
                        filterableFieldsSet,
                        numElementToSkip,
                        Long.MAX_VALUE,
                        partitions,
                        readableMetadata,
                        null);
            }

            if (!enableLookup) {
                if (enableWatermarkPushDown) {
                    return new TestValuesScanTableSourceWithWatermarkPushDown(
                            producedDataType,
                            changelogMode,
                            runtimeSource,
                            failingSource,
                            partition2Rows,
                            context.getObjectIdentifier().getObjectName(),
                            nestedProjectionSupported,
                            null,
                            Collections.emptyList(),
                            filterableFieldsSet,
                            numElementToSkip,
                            Long.MAX_VALUE,
                            partitions,
                            readableMetadata,
                            null);
                } else {
                    return new TestValuesScanTableSource(
                            producedDataType,
                            changelogMode,
                            isBounded,
                            runtimeSource,
                            failingSource,
                            partition2Rows,
                            nestedProjectionSupported,
                            null,
                            Collections.emptyList(),
                            filterableFieldsSet,
                            numElementToSkip,
                            Long.MAX_VALUE,
                            partitions,
                            readableMetadata,
                            null);
                }
            } else {
                return new TestValuesScanLookupTableSource(
                        producedDataType,
                        changelogMode,
                        isBounded,
                        runtimeSource,
                        failingSource,
                        partition2Rows,
                        isAsync,
                        lookupFunctionClass,
                        nestedProjectionSupported,
                        null,
                        Collections.emptyList(),
                        filterableFieldsSet,
                        numElementToSkip,
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
        Integer parallelism = helper.getOptions().get(SINK_PARALLELISM);
        boolean dropLateEvent = helper.getOptions().get(SINK_DROP_LATE_EVENT);
        final Map<String, DataType> writableMetadata =
                TestValuesFactoryUtils.convertToMetadataMap(
                        helper.getOptions().get(WRITABLE_METADATA), context.getClassLoader());
        final ChangelogMode changelogMode =
                Optional.ofNullable(helper.getOptions().get(SINK_CHANGELOG_MODE_ENFORCED))
                        .map(m -> TestValuesFactoryUtils.parseChangelogMode(m))
                        .orElse(null);

        final DataType consumedType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        final int[] primaryKeyIndices =
                TableSchemaUtils.getPrimaryKeyIndices(context.getCatalogTable().getSchema());

        if (sinkClass.equals("DEFAULT")) {
            int rowTimeIndex =
                    TestValuesFactoryUtils.validateAndExtractRowtimeIndex(
                            context.getCatalogTable(), dropLateEvent, isInsertOnly);
            return new TestValuesTableSink(
                    consumedType,
                    primaryKeyIndices,
                    context.getObjectIdentifier().getObjectName(),
                    isInsertOnly,
                    runtimeSink,
                    expectedNum,
                    writableMetadata,
                    parallelism,
                    changelogMode,
                    rowTimeIndex);
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
        return new HashSet<>(
                Arrays.asList(
                        DATA_ID,
                        CHANGELOG_MODE,
                        BOUNDED,
                        RUNTIME_SOURCE,
                        TABLE_SOURCE_CLASS,
                        FAILING_SOURCE,
                        LOOKUP_FUNCTION_CLASS,
                        ASYNC_ENABLED,
                        ENABLE_LOOKUP,
                        TABLE_SOURCE_CLASS,
                        TABLE_SINK_CLASS,
                        SINK_INSERT_ONLY,
                        RUNTIME_SINK,
                        SINK_EXPECTED_MESSAGES_NUM,
                        ENABLE_PROJECTION_PUSH_DOWN,
                        NESTED_PROJECTION_SUPPORTED,
                        FILTERABLE_FIELDS,
                        PARTITION_LIST,
                        READABLE_METADATA,
                        SINK_PARALLELISM,
                        SINK_CHANGELOG_MODE_ENFORCED,
                        WRITABLE_METADATA,
                        ENABLE_WATERMARK_PUSH_DOWN,
                        SINK_DROP_LATE_EVENT,
                        SOURCE_NUM_ELEMENT_TO_SKIP,
                        INTERNAL_DATA));
    }
}
