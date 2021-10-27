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

    private List<Map<String, String>> remainingPartitions;
    private List<ResolvedExpression> filters;
    private Long limit;

    private DataType fullOutputDataType;
    private List<String> metadataKeys;
    private int[][] projectFields;

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

        this.fullOutputDataType = context.getPhysicalRowDataType();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // When this table has no partition, just return a empty source.
        if (!partitionKeys.isEmpty() && getOrFetchPartitions().isEmpty()) {
            return InputFormatProvider.of(new CollectionInputFormat<>(new ArrayList<>(), null));
        }

        // Compute output type and physical type
        DataType fullOutputDataType = this.fullOutputDataType;
        if (projectFields != null) {
            fullOutputDataType = DataType.projectFields(fullOutputDataType, projectFields);
        }

        // Physical type is computed from the full data type, filtering out partition and
        // metadata columns
        DataType physicalDataType =
                DataTypes.ROW(
                        DataType.getFields(fullOutputDataType).stream()
                                .filter(
                                        f -> {
                                            if (partitionKeys != null
                                                    && partitionKeys.contains(f.getName())) {
                                                return false;
                                            }

                                            if (metadataKeys != null
                                                    && metadataKeys.contains(f.getName())) {
                                                return false;
                                            }

                                            return true;
                                        })
                                .toArray(DataTypes.Field[]::new));

        // Resolve metadata and make sure to filter out metadata not in the full output type
        List<String> metadataKeys =
                (this.metadataKeys == null) ? Collections.emptyList() : this.metadataKeys;
        metadataKeys =
                DataType.getFieldNames(fullOutputDataType).stream()
                        .filter(metadataKeys::contains)
                        .collect(Collectors.toList());
        List<ReadableFileInfo> metadataToExtract =
                metadataKeys.stream().map(ReadableFileInfo::resolve).collect(Collectors.toList());

        // Filter out partition columns not in full output type
        List<String> partitionKeysToExtract =
                DataType.getFieldNames(fullOutputDataType).stream()
                        .filter(this.partitionKeys::contains)
                        .collect(Collectors.toList());

        // TODO FLINK-19845 old format factory, to be removed soon. The old factory doesn't support
        //  metadata.
        if (formatFactory != null) {
            if (!metadataToExtract.isEmpty()) {
                throw new IllegalStateException(
                        "Metadata are not supported for format factories using FileSystemFormatFactory");
            }

            // The ContinuousFileMonitoringFunction can not accept multiple paths. Default
            // StreamEnv.createInput will create continuous function.
            // Avoid using ContinuousFileMonitoringFunction.
            return SourceFunctionProvider.of(
                    new InputFormatSourceFunction<>(
                            getInputFormat(),
                            InternalTypeInfo.of(fullOutputDataType.getLogicalType())),
                    true);
        }

        if (bulkReaderFormat != null) {
            if (bulkReaderFormat instanceof BulkDecodingFormat
                    && filters != null
                    && filters.size() > 0) {
                ((BulkDecodingFormat<RowData>) bulkReaderFormat).applyFilters(filters);
            }
            BulkFormat<RowData, FileSourceSplit> bulkFormat =
                    wrapBulkFormat(
                            bulkReaderFormat.createRuntimeDecoder(scanContext, physicalDataType),
                            fullOutputDataType,
                            metadataToExtract,
                            partitionKeysToExtract);
            return createSourceProvider(bulkFormat);
        } else if (deserializationFormat != null) {
            DeserializationSchema<RowData> decoder =
                    deserializationFormat.createRuntimeDecoder(scanContext, physicalDataType);
            BulkFormat<RowData, FileSourceSplit> bulkFormat =
                    wrapBulkFormat(
                            new DeserializationSchemaAdapter(decoder),
                            fullOutputDataType,
                            metadataToExtract,
                            partitionKeysToExtract);
            return createSourceProvider(bulkFormat);
        } else {
            throw new TableException("Can not find format factory.");
        }
    }

    /**
     * Wraps bulk format in a {@link FileInfoExtractorBulkFormat} and {@link LimitableBulkFormat},
     * if needed.
     */
    private BulkFormat<RowData, FileSourceSplit> wrapBulkFormat(
            BulkFormat<RowData, FileSourceSplit> bulkFormat,
            DataType fullOutputDataType,
            List<ReadableFileInfo> metadata,
            List<String> partitionKeys) {
        if (!metadata.isEmpty() || !partitionKeys.isEmpty()) {
            bulkFormat =
                    new FileInfoExtractorBulkFormat(
                            bulkFormat,
                            fullOutputDataType,
                            metadata.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    ReadableFileInfo::getKey,
                                                    ReadableFileInfo::getAccessor)),
                            partitionKeys,
                            defaultPartName);
        }
        bulkFormat = LimitableBulkFormat.create(bulkFormat, limit);
        return bulkFormat;
    }

    private SourceProvider createSourceProvider(BulkFormat<RowData, FileSourceSplit> bulkFormat) {
        return SourceProvider.of(FileSource.forBulkFileFormat(bulkFormat, paths()).build());
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
                        return projectFields == null
                                ? IntStream.range(0, DataType.getFieldCount(getPhysicalDataType()))
                                        .toArray()
                                : Arrays.stream(projectFields)
                                        .mapToInt(array -> array[0])
                                        .toArray();
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
    public FileSystemTableSource copy() {
        FileSystemTableSource source =
                new FileSystemTableSource(
                        context, bulkReaderFormat, deserializationFormat, formatFactory);
        source.remainingPartitions = remainingPartitions;
        source.filters = filters;
        source.limit = limit;

        source.projectFields = projectFields;
        source.metadataKeys = metadataKeys;
        source.partitionKeys = partitionKeys;
        source.fullOutputDataType = fullOutputDataType;
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

    // --------------------------------------------------------------------------------------------
    // Methods to apply projections and metadata,
    // will influence the final output and physical type used by formats
    // --------------------------------------------------------------------------------------------

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.projectFields = projectedFields;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        if (!metadataKeys.isEmpty()) {
            this.metadataKeys = metadataKeys;
            this.fullOutputDataType = producedDataType;
            // If a projection was pushed down, we need to remove it here because producedDataType
            // is already projected
            if (projectFields != null) {
                projectFields = null;
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Arrays.stream(ReadableFileInfo.values())
                .collect(Collectors.toMap(ReadableFileInfo::getKey, ReadableFileInfo::getDataType));
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
