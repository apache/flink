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

package org.apache.flink.table.planner.factories.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.functions.aggfunctions.Count1AggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.CountAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MaxAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.MinAggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction;
import org.apache.flink.table.planner.functions.aggfunctions.SumAggFunction;
import org.apache.flink.table.planner.runtime.utils.FailingCollectionSource;
import org.apache.flink.table.planner.utils.FilterUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.factories.TestValuesFactoryUtils.mapPartitionToRow;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Values {@link ScanTableSource} for testing that disables projection push down. */
public class TestValuesScanTableSourceWithoutProjectionPushDown
        implements ScanTableSource,
                SupportsFilterPushDown,
                SupportsLimitPushDown,
                SupportsPartitionPushDown,
                SupportsReadingMetadata,
                SupportsAggregatePushDown {

    protected DataType producedDataType;
    protected DataType originDataType;
    protected final ChangelogMode changelogMode;
    protected final boolean bounded;
    protected final String runtimeSource;
    protected final boolean failingSource;
    protected Map<Map<String, String>, Collection<Row>> data;

    protected final boolean nestedProjectionSupported;
    protected @Nullable int[][] projectedPhysicalFields;
    protected List<ResolvedExpression> filterPredicates;
    protected final Set<String> filterableFields;
    protected long limit;
    protected int numElementToSkip;
    protected List<Map<String, String>> allPartitions;
    protected final Map<String, DataType> readableMetadata;
    protected @Nullable int[] projectedMetadataFields;

    private @Nullable int[] groupingSet;
    private List<AggregateExpression> aggregateExpressions;

    public TestValuesScanTableSourceWithoutProjectionPushDown(
            DataType producedDataType,
            ChangelogMode changelogMode,
            boolean bounded,
            String runtimeSource,
            boolean failingSource,
            Map<Map<String, String>, Collection<Row>> data,
            boolean nestedProjectionSupported,
            @Nullable int[][] projectedPhysicalFields,
            List<ResolvedExpression> filterPredicates,
            Set<String> filterableFields,
            int numElementToSkip,
            long limit,
            List<Map<String, String>> allPartitions,
            Map<String, DataType> readableMetadata,
            @Nullable int[] projectedMetadataFields) {
        this.producedDataType = producedDataType;
        this.originDataType = producedDataType;
        this.changelogMode = changelogMode;
        this.bounded = bounded;
        this.runtimeSource = runtimeSource;
        this.failingSource = failingSource;
        this.data = data;
        this.nestedProjectionSupported = nestedProjectionSupported;
        this.projectedPhysicalFields = projectedPhysicalFields;
        this.filterPredicates = filterPredicates;
        this.filterableFields = filterableFields;
        this.numElementToSkip = numElementToSkip;
        this.limit = limit;
        this.allPartitions = allPartitions;
        this.readableMetadata = readableMetadata;
        this.projectedMetadataFields = projectedMetadataFields;
        this.groupingSet = null;
        this.aggregateExpressions = Collections.emptyList();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return changelogMode;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        TypeInformation<RowData> type =
                runtimeProviderContext.createTypeInformation(producedDataType);
        TypeSerializer<RowData> serializer = type.createSerializer(new ExecutionConfig());
        DataStructureConverter converter =
                runtimeProviderContext.createDataStructureConverter(producedDataType);
        converter.open(
                RuntimeConverter.Context.create(TestValuesTableFactory.class.getClassLoader()));
        Collection<RowData> values = convertToRowData(converter, true);

        switch (runtimeSource) {
            case "SourceFunction":
                try {
                    final SourceFunction<RowData> sourceFunction;
                    if (failingSource) {
                        sourceFunction =
                                new FailingCollectionSource<>(
                                        serializer, values, values.size() / 2);
                    } else {
                        sourceFunction = new FromElementsFunction<>(serializer, values);
                    }
                    return SourceFunctionProvider.of(sourceFunction, bounded);
                } catch (IOException e) {
                    throw new TableException("Fail to init source function", e);
                }
            case "InputFormat":
                checkArgument(
                        !failingSource,
                        "Values InputFormat Source doesn't support as failing source.");
                return InputFormatProvider.of(new CollectionInputFormat<>(values, serializer));
            case "DataStream":
                checkArgument(
                        !failingSource,
                        "Values DataStream Source doesn't support as failing source.");
                try {
                    FromElementsFunction<RowData> function =
                            new FromElementsFunction<>(serializer, values);
                    return new DataStreamScanProvider() {
                        @Override
                        public DataStream<RowData> produceDataStream(
                                StreamExecutionEnvironment execEnv) {
                            return execEnv.addSource(function);
                        }

                        @Override
                        public boolean isBounded() {
                            return bounded;
                        }
                    };
                } catch (IOException e) {
                    throw new TableException("Fail to init data stream source", e);
                }
            case "Source":
                try {
                    // In a new source, watermark must be produced before PushFilterDown, so should
                    // get the origin type and rows that have not been projected to test.
                    TypeInformation<RowData> originType =
                            runtimeProviderContext.createTypeInformation(originDataType);
                    TypeSerializer<RowData> originTypeSerializer =
                            originType.createSerializer(new ExecutionConfig());
                    DataStructureConverter originTypeConverter =
                            runtimeProviderContext.createDataStructureConverter(originDataType);
                    converter.open(
                            RuntimeConverter.Context.create(
                                    TestValuesTableFactory.class.getClassLoader()));

                    return SourceProvider.of(
                            new TestValuesSource(
                                    originTypeSerializer,
                                    convertToRowData(originTypeConverter, false),
                                    values.size()));
                } catch (IOException e) {
                    throw new TableException("Fail to init source", e);
                }
            default:
                throw new IllegalArgumentException(
                        "Unsupported runtime source class: " + runtimeSource);
        }
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

        this.data = filterAllData(this.data);

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
        return new TestValuesScanTableSourceWithoutProjectionPushDown(
                producedDataType,
                changelogMode,
                bounded,
                runtimeSource,
                failingSource,
                data,
                nestedProjectionSupported,
                projectedPhysicalFields,
                filterPredicates,
                filterableFields,
                numElementToSkip,
                limit,
                allPartitions,
                readableMetadata,
                projectedMetadataFields);
    }

    @Override
    public String asSummaryString() {
        return "TestValues";
    }

    protected Collection<RowData> convertToRowData(
            DataStructureConverter converter, boolean applyProjectionAndAggregate) {
        List<RowData> result = new ArrayList<>();
        int numSkipped = 0;
        for (Map<String, String> partition : data.keySet()) {
            Collection<Row> rowsInPartition = data.get(partition);

            // handle element skipping
            int numToSkipInPartition = 0;
            if (numSkipped < numElementToSkip) {
                numToSkipInPartition =
                        Math.min(rowsInPartition.size(), numElementToSkip - numSkipped);
            }
            numSkipped += numToSkipInPartition;

            // handle projection
            // Now we don't do it in applyProjections and applyReadableMetadata like
            // applyPartitions and applyFilters, Because in periods before we can't confirm the
            // final fields.
            List<Row> rowsRetained =
                    rowsInPartition.stream()
                            .skip(numToSkipInPartition)
                            .map(
                                    row -> {
                                        if (applyProjectionAndAggregate) {
                                            Row projectedRow = projectRow(row);
                                            projectedRow.setKind(row.getKind());
                                            return projectedRow;
                                        } else {
                                            return row;
                                        }
                                    })
                            .collect(Collectors.toList());
            // handle aggregates
            if (applyProjectionAndAggregate && !aggregateExpressions.isEmpty()) {
                rowsRetained = applyAggregatesToRows(rowsRetained);
            }

            // handle row data
            for (Row row : rowsRetained) {
                final RowData rowData = (RowData) converter.toInternal(row);
                if (rowData != null) {
                    rowData.setRowKind(row.getKind());
                    result.add(rowData);
                }

                // handle limit. No aggregates will be pushed down when there is a limit.
                if (result.size() >= limit) {
                    return result;
                }
            }
        }

        return result;
    }

    private List<Row> applyAggregatesToRows(List<Row> rows) {
        if (groupingSet != null && groupingSet.length > 0) {
            // has group by, group firstly
            Map<Row, List<Row>> buffer = new HashMap<>();
            for (Row row : rows) {
                Row bufferKey = new Row(groupingSet.length);
                for (int i = 0; i < groupingSet.length; i++) {
                    bufferKey.setField(i, row.getField(groupingSet[i]));
                }
                if (buffer.containsKey(bufferKey)) {
                    buffer.get(bufferKey).add(row);
                } else {
                    buffer.put(bufferKey, new ArrayList<>(Collections.singletonList(row)));
                }
            }
            List<Row> result = new ArrayList<>();
            for (Map.Entry<Row, List<Row>> entry : buffer.entrySet()) {
                result.add(Row.join(entry.getKey(), accumulateRows(entry.getValue())));
            }
            return result;
        } else {
            return Collections.singletonList(accumulateRows(rows));
        }
    }

    // can only apply sum/sum0/avg function for long type fields for testing
    private Row accumulateRows(List<Row> rows) {
        Row result = new Row(aggregateExpressions.size());
        for (int i = 0; i < aggregateExpressions.size(); i++) {
            FunctionDefinition aggFunction = aggregateExpressions.get(i).getFunctionDefinition();
            List<FieldReferenceExpression> arguments = aggregateExpressions.get(i).getArgs();
            if (aggFunction instanceof MinAggFunction) {
                int argIndex = arguments.get(0).getFieldIndex();
                Row minRow =
                        rows.stream()
                                .min(Comparator.comparing(row -> row.getFieldAs(argIndex)))
                                .orElse(null);
                result.setField(i, minRow != null ? minRow.getField(argIndex) : null);
            } else if (aggFunction instanceof MaxAggFunction) {
                int argIndex = arguments.get(0).getFieldIndex();
                Row maxRow =
                        rows.stream()
                                .max(Comparator.comparing(row -> row.getFieldAs(argIndex)))
                                .orElse(null);
                result.setField(i, maxRow != null ? maxRow.getField(argIndex) : null);
            } else if (aggFunction instanceof SumAggFunction) {
                int argIndex = arguments.get(0).getFieldIndex();
                Object finalSum =
                        rows.stream()
                                .filter(row -> row.getField(argIndex) != null)
                                .mapToLong(row -> row.getFieldAs(argIndex))
                                .sum();

                boolean allNull = rows.stream().noneMatch(r -> r.getField(argIndex) != null);
                result.setField(i, allNull ? null : finalSum);
            } else if (aggFunction instanceof Sum0AggFunction) {
                int argIndex = arguments.get(0).getFieldIndex();
                Object finalSum0 =
                        rows.stream()
                                .filter(row -> row.getField(argIndex) != null)
                                .mapToLong(row -> row.getFieldAs(argIndex))
                                .sum();
                result.setField(i, finalSum0);
            } else if (aggFunction instanceof CountAggFunction) {
                int argIndex = arguments.get(0).getFieldIndex();
                long count = rows.stream().filter(r -> r.getField(argIndex) != null).count();
                result.setField(i, count);
            } else if (aggFunction instanceof Count1AggFunction) {
                result.setField(i, (long) rows.size());
            }
        }
        return result;
    }

    private Map<Map<String, String>, Collection<Row>> filterAllData(
            Map<Map<String, String>, Collection<Row>> allData) {
        final Map<Map<String, String>, Collection<Row>> result = new HashMap<>();

        for (Map<String, String> partition : allData.keySet()) {
            List<Row> remainData = new ArrayList<>();
            for (Row row : allData.get(partition)) {
                boolean isRetained =
                        FilterUtils.isRetainedAfterApplyingFilterPredicates(
                                filterPredicates, getValueGetter(row));
                if (isRetained) {
                    remainData.add(row);
                }
            }
            result.put(partition, remainData);
        }
        return result;
    }

    private Row projectRow(Row row) {
        if (projectedPhysicalFields == null) {
            return row;
        }
        int originPhysicalSize = row.getArity() - readableMetadata.size();
        int newLength =
                projectedPhysicalFields.length
                        + (projectedMetadataFields == null ? 0 : projectedMetadataFields.length);
        Object[] newValues = new Object[newLength];
        for (int i = 0; i < projectedPhysicalFields.length; i++) {
            Object field = row;
            for (int dim : projectedPhysicalFields[i]) {
                field = ((Row) field).getField(dim);
            }
            newValues[i] = field;
        }
        for (int i = projectedPhysicalFields.length; i < newValues.length; i++) {
            newValues[i] =
                    row.getField(
                            projectedMetadataFields[i - projectedPhysicalFields.length]
                                    + originPhysicalSize);
        }
        return Row.of(newValues);
    }

    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        if (allPartitions.isEmpty()) {
            return Optional.empty();
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
                this.data =
                        mapPartitionToRow(
                                producedDataType,
                                data.get(Collections.EMPTY_MAP),
                                remainingPartitions);
            } else {
                // we will read data from Collections.emptyList() if allPartitions is empty.
                // therefore, we should clear all data manually.
                remainingPartitions = (List<Map<String, String>>) Collections.emptyMap();
                this.data.put(Collections.emptyMap(), Collections.emptyList());
            }

        } else {
            this.allPartitions = remainingPartitions;
            if (remainingPartitions.isEmpty()) {
                remainingPartitions = (List<Map<String, String>>) Collections.emptyMap();
            }
        }
        // only keep the data in the remaining partitions
        this.data = pruneDataByRemainingPartitions(remainingPartitions, this.data);
    }

    private Map<Map<String, String>, Collection<Row>> pruneDataByRemainingPartitions(
            List<Map<String, String>> remainingPartitions,
            Map<Map<String, String>, Collection<Row>> allData) {
        Map<Map<String, String>, Collection<Row>> result = new HashMap<>();
        for (Map<String, String> remainingPartition : remainingPartitions) {
            result.put(remainingPartition, allData.get(remainingPartition));
        }
        return result;
    }

    @Override
    public boolean applyAggregates(
            List<int[]> groupingSets,
            List<AggregateExpression> aggregateExpressions,
            DataType producedDataType) {
        // This TestValuesScanTableSource only supports single group aggregate ar present.
        if (groupingSets.size() > 1) {
            return false;
        }
        List<AggregateExpression> aggExpressions = new ArrayList<>();
        for (AggregateExpression aggExpression : aggregateExpressions) {
            FunctionDefinition functionDefinition = aggExpression.getFunctionDefinition();
            if (!(functionDefinition instanceof MinAggFunction
                    || functionDefinition instanceof MaxAggFunction
                    || functionDefinition instanceof SumAggFunction
                    || functionDefinition instanceof Sum0AggFunction
                    || functionDefinition instanceof CountAggFunction
                    || functionDefinition instanceof Count1AggFunction)) {
                return false;
            }
            if (aggExpression.getFilterExpression().isPresent()
                    || aggExpression.isApproximate()
                    || aggExpression.isDistinct()) {
                return false;
            }

            // only Long data type is supported in this unit test expect count()
            if (aggExpression.getArgs().stream()
                    .anyMatch(
                            field ->
                                    !(field.getOutputDataType().getLogicalType()
                                                    instanceof BigIntType)
                                            && !(functionDefinition instanceof CountAggFunction
                                                    || functionDefinition
                                                            instanceof Count1AggFunction))) {
                return false;
            }

            aggExpressions.add(aggExpression);
        }
        this.groupingSet = groupingSets.get(0);
        this.aggregateExpressions = aggExpressions;
        this.producedDataType = producedDataType;
        return true;
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
    public void applyReadableMetadata(
            List<String> remainingMetadataKeys, DataType newProducedDataType) {
        producedDataType = newProducedDataType;
        final List<String> allMetadataKeys = new ArrayList<>(listReadableMetadata().keySet());
        projectedMetadataFields =
                remainingMetadataKeys.stream().mapToInt(allMetadataKeys::indexOf).toArray();
    }
}
