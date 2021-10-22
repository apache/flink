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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.BulkDecodingFormat;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.apache.flink.table.utils.TableSchemaUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** File system table source. */
public class FileSystemTableSource extends AbstractFileSystemTable
        implements ScanTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown,
                SupportsPartitionPushDown,
                SupportsFilterPushDown,
                SupportsReadingMetadata {

    @Nullable private final DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat;
    @Nullable private final DecodingFormat<DeserializationSchema<RowData>> deserializationFormat;
    @Nullable private final FileSystemFormatFactory formatFactory;

    private int[][] projectedFields;
    private List<Map<String, String>> remainingPartitions;
    private List<ResolvedExpression> filters;
    private Long limit;

    // Metadata related fields
    private DataType producedDataType;
    private List<ReadableFileInfo> usedMetadata;
    private List<String> usedMetadataKeys;

    public FileSystemTableSource(
            DynamicTableFactory.Context context,
            @Nullable DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> deserializationFormat,
            @Nullable FileSystemFormatFactory formatFactory) {
        super(context);
        if (Stream.of(bulkReaderFormat, deserializationFormat, formatFactory)
                .allMatch(Objects::isNull)) {
            Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
            String identifier = options.get(FactoryUtil.FORMAT);
            throw new ValidationException(
                    String.format(
                            "Could not find any format factory for identifier '%s' in the classpath.",
                            identifier));
        }
        this.bulkReaderFormat = bulkReaderFormat;
        this.deserializationFormat = deserializationFormat;
        this.formatFactory = formatFactory;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        if (!partitionKeys.isEmpty() && getOrFetchPartitions().isEmpty()) {
            // When this table has no partition, just return a empty source.
            return InputFormatProvider.of(new CollectionInputFormat<>(new ArrayList<>(), null));
        } else if (bulkReaderFormat != null) {
            if (bulkReaderFormat instanceof BulkDecodingFormat
                    && filters != null
                    && filters.size() > 0) {
                ((BulkDecodingFormat<RowData>) bulkReaderFormat).applyFilters(filters);
            }
            BulkFormat<RowData, FileSourceSplit> bulkFormat =
                    bulkReaderFormat.createRuntimeDecoder(scanContext, getProjectedDataType());
            return createSourceProvider(bulkFormat);
        } else if (formatFactory != null) {
            // The ContinuousFileMonitoringFunction can not accept multiple paths. Default
            // StreamEnv.createInput will create continuous function.
            // Avoid using ContinuousFileMonitoringFunction.
            return SourceFunctionProvider.of(
                    new InputFormatSourceFunction<>(
                            getInputFormat(),
                            InternalTypeInfo.of(getProjectedDataType().getLogicalType())),
                    true);
        } else if (deserializationFormat != null) {
            // NOTE, we need pass full format types to deserializationFormat
            DeserializationSchema<RowData> decoder =
                    deserializationFormat.createRuntimeDecoder(
                            scanContext, getPhysicalDataTypeWithoutPartitionColumns());
            return createSourceProvider(
                    new DeserializationSchemaAdapter(
                            decoder,
                            getPhysicalDataType(),
                            readFields(),
                            partitionKeys,
                            defaultPartName));
            // return sourceProvider(wrapDeserializationFormat(deserializationFormat), scanContext);
        } else {
            throw new TableException("Can not find format factory.");
        }
    }

    private SourceProvider createSourceProvider(BulkFormat<RowData, FileSourceSplit> bulkFormat) {
        if (this.usedMetadata != null) {
            bulkFormat =
                    new FileInfoExtractorBulkFormat(
                            bulkFormat,
                            this.producedDataType,
                            InternalTypeInfo.of(this.producedDataType.getLogicalType()),
                            usedMetadata.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    ReadableFileInfo::getKey,
                                                    ReadableFileInfo::getAccessor)));
        }
        FileSource.FileSourceBuilder<RowData> builder =
                FileSource.forBulkFileFormat(
                        LimitableBulkFormat.create(bulkFormat, limit), paths());
        return SourceProvider.of(builder.build());
    }

    private Path[] paths() {
        if (partitionKeys.isEmpty()) {
            return new Path[] {path};
        } else {
            return getOrFetchPartitions().stream()
                    .map(FileSystemTableSource.this::toFullLinkedPartSpec)
                    .map(PartitionPathUtils::generatePartitionPath)
                    .map(n -> new Path(path, n))
                    .toArray(Path[]::new);
        }
    }

    private InputFormat<RowData, ?> getInputFormat() {
        return formatFactory.createReader(
                new FileSystemFormatFactory.ReaderContext() {

                    @Override
                    public TableSchema getSchema() {
                        return TableSchemaUtils.getPhysicalSchema(
                                TableSchema.fromResolvedSchema(schema));
                    }

                    @Override
                    public ReadableConfig getFormatOptions() {
                        return formatOptions(formatFactory.factoryIdentifier());
                    }

                    @Override
                    public List<String> getPartitionKeys() {
                        return partitionKeys;
                    }

                    @Override
                    public String getDefaultPartName() {
                        return defaultPartName;
                    }

                    @Override
                    public Path[] getPaths() {
                        return paths();
                    }

                    @Override
                    public int[] getProjectFields() {
                        return readFields();
                    }

                    @Override
                    public long getPushedDownLimit() {
                        return limit == null ? Long.MAX_VALUE : limit;
                    }

                    @Override
                    public List<ResolvedExpression> getPushedDownFilters() {
                        return filters == null ? Collections.emptyList() : filters;
                    }
                });
    }

    @Override
    public ChangelogMode getChangelogMode() {
        if (bulkReaderFormat != null) {
            return bulkReaderFormat.getChangelogMode();
        } else if (formatFactory != null) {
            return ChangelogMode.insertOnly();
        } else if (deserializationFormat != null) {
            return deserializationFormat.getChangelogMode();
        } else {
            throw new TableException("Can not find format factory.");
        }
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        this.filters = new ArrayList<>(filters);
        return Result.of(new ArrayList<>(filters), new ArrayList<>(filters));
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        try {
            return Optional.of(
                    PartitionPathUtils.searchPartSpecAndPaths(
                                    path.getFileSystem(), path, partitionKeys.size())
                            .stream()
                            .map(tuple2 -> tuple2.f0)
                            .map(
                                    spec -> {
                                        LinkedHashMap<String, String> ret = new LinkedHashMap<>();
                                        spec.forEach(
                                                (k, v) ->
                                                        ret.put(
                                                                k,
                                                                defaultPartName.equals(v)
                                                                        ? null
                                                                        : v));
                                        return ret;
                                    })
                            .collect(Collectors.toList()));
        } catch (Exception e) {
            throw new TableException("Fetch partitions fail.", e);
        }
    }

    @Override
    public void applyPartitions(List<Map<String, String>> remainingPartitions) {
        this.remainingPartitions = remainingPartitions;
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.projectedFields = projectedFields;
    }

    @Override
    public FileSystemTableSource copy() {
        FileSystemTableSource source =
                new FileSystemTableSource(
                        context, bulkReaderFormat, deserializationFormat, formatFactory);
        source.projectedFields = projectedFields;
        source.remainingPartitions = remainingPartitions;
        source.filters = filters;
        source.limit = limit;

        source.producedDataType = producedDataType;
        source.usedMetadata = usedMetadata;
        source.usedMetadataKeys = usedMetadataKeys;
        return source;
    }

    @Override
    public String asSummaryString() {
        return "Filesystem";
    }

    private List<Map<String, String>> getOrFetchPartitions() {
        if (remainingPartitions == null) {
            remainingPartitions = listPartitions().get();
        }
        return remainingPartitions;
    }

    private LinkedHashMap<String, String> toFullLinkedPartSpec(Map<String, String> part) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        for (String k : partitionKeys) {
            if (!part.containsKey(k)) {
                throw new TableException(
                        "Partition keys are: "
                                + partitionKeys
                                + ", incomplete partition spec: "
                                + part);
            }
            map.put(k, part.get(k));
        }
        return map;
    }

    private int[] readFields() {
        if (this.producedDataType != null) {
            return IntStream.range(
                            0,
                            (int)
                                    DataType.getFields(this.producedDataType).stream()
                                            .filter(
                                                    field ->
                                                            !usedMetadataKeys.contains(
                                                                    field.getName()))
                                            .count())
                    .toArray();
        }
        return projectedFields == null
                ? IntStream.range(0, DataType.getFieldCount(getPhysicalDataType())).toArray()
                : Arrays.stream(projectedFields).mapToInt(array -> array[0]).toArray();
    }

    @Override
    DataType getPhysicalDataType() {
        if (this.usedMetadataKeys != null) {
            return DataTypes.ROW(
                    DataType.getFields(super.getPhysicalDataType()).stream()
                            .filter(field -> !usedMetadataKeys.contains(field.getName()))
                            .toArray(DataTypes.Field[]::new));
        }
        return super.getPhysicalDataType();
    }

    private DataType getProjectedDataType() {
        final DataType physicalDataType =
                this.producedDataType != null
                        ? DataTypes.ROW(
                                DataType.getFields(this.producedDataType).stream()
                                        .filter(
                                                field ->
                                                        !usedMetadataKeys.contains(field.getName()))
                                        .toArray(DataTypes.Field[]::new))
                        : super.getPhysicalDataType();

        // If we haven't projected fields, we just return the original physical data type,
        // otherwise we need to compute the physical data type depending on the projected fields.
        if (projectedFields == null) {
            return physicalDataType;
        }
        return DataType.projectFields(physicalDataType, projectedFields);
    }

    @Override
    DataType getPhysicalDataTypeWithoutPartitionColumns() {
        if (this.producedDataType != null) {
            return DataTypes.ROW(
                    DataType.getFields(this.producedDataType).stream()
                            .filter(field -> !usedMetadataKeys.contains(field.getName()))
                            .filter(field -> !partitionKeys.contains(field.getName()))
                            .toArray(DataTypes.Field[]::new));
        }
        return super.getPhysicalDataTypeWithoutPartitionColumns();
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Arrays.stream(ReadableFileInfo.values())
                .collect(Collectors.toMap(ReadableFileInfo::getKey, ReadableFileInfo::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        if (metadataKeys.isEmpty()) {
            // This method should be idempotent
            this.usedMetadataKeys = null;
            this.usedMetadata = null;
            this.producedDataType = null;
            return;
        }

        this.usedMetadataKeys = metadataKeys;
        this.usedMetadata =
                metadataKeys.stream().map(ReadableFileInfo::resolve).collect(Collectors.toList());
        this.producedDataType = producedDataType;
    }

    interface FileInfoAccessor extends Serializable {
        /**
         * Access the information from the {@link org.apache.flink.core.fs.FileInputSplit}. The
         * return value type must be an internal type.
         */
        Object getValue(FileSourceSplit split);
    }

    enum ReadableFileInfo implements Serializable {
        FILEPATH(
                "filepath",
                DataTypes.STRING().notNull(),
                new FileInfoAccessor() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object getValue(FileSourceSplit split) {
                        return StringData.fromString(split.path().getPath());
                    }
                });

        final String key;
        final DataType dataType;
        final FileInfoAccessor converter;

        ReadableFileInfo(String key, DataType dataType, FileInfoAccessor converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }

        public String getKey() {
            return key;
        }

        public DataType getDataType() {
            return dataType;
        }

        public FileInfoAccessor getAccessor() {
            return converter;
        }

        public static ReadableFileInfo resolve(String key) {
            switch (key) {
                case "filepath":
                    return ReadableFileInfo.FILEPATH;
                default:
                    throw new IllegalArgumentException(
                            "Cannot resolve the provided ReadableMetadata key");
            }
        }
    }
}
