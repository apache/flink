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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.table.format.BulkDecodingFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.FileBasedStatisticsReportableInputFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsStatisticReport;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.util.CollectionUtil.entry;

/** File system table source. */
@Internal
public class FileSystemTableSource extends AbstractFileSystemTable
        implements ScanTableSource,
                LookupTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown,
                SupportsPartitionPushDown,
                SupportsFilterPushDown,
                SupportsReadingMetadata,
                SupportsStatisticReport {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemTableSource.class);

    @Nullable protected final DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat;
    @Nullable protected final DecodingFormat<DeserializationSchema<RowData>> deserializationFormat;

    // These mutable fields
    protected List<Map<String, String>> remainingPartitions;
    protected List<ResolvedExpression> filters;
    protected Long limit;
    protected int[][] projectFields;
    protected List<String> metadataKeys;
    protected DataType producedDataType;

    private final int lookupMaxRetryTimes;

    public FileSystemTableSource(
            ObjectIdentifier tableIdentifier,
            DataType physicalRowDataType,
            List<String> partitionKeys,
            ReadableConfig tableOptions,
            @Nullable DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> deserializationFormat,
            int lookupMaxRetryTimes) {
        super(tableIdentifier, physicalRowDataType, partitionKeys, tableOptions);
        if (Stream.of(bulkReaderFormat, deserializationFormat).allMatch(Objects::isNull)) {
            String identifier = tableOptions.get(FactoryUtil.FORMAT);
            throw new ValidationException(
                    String.format(
                            "Could not find any format factory for identifier '%s' in the classpath.",
                            identifier));
        }
        this.bulkReaderFormat = bulkReaderFormat;
        this.deserializationFormat = deserializationFormat;
        this.producedDataType = physicalRowDataType;
        this.lookupMaxRetryTimes = lookupMaxRetryTimes;
    }

    private BulkFormat<RowData, FileSourceSplit> createFormat(
            DynamicTableSource.Context sourceContext) {
        // Resolve metadata and make sure to filter out metadata not in the producedDataType
        final List<String> metadataKeys =
                DataType.getFieldNames(producedDataType).stream()
                        .filter(
                                ((this.metadataKeys == null)
                                                ? Collections.emptyList()
                                                : this.metadataKeys)
                                        ::contains)
                        .collect(Collectors.toList());
        final List<ReadableFileInfo> metadataToExtract =
                metadataKeys.stream().map(ReadableFileInfo::resolve).collect(Collectors.toList());

        // Filter out partition columns not in producedDataType
        final List<String> partitionKeysToExtract =
                DataType.getFieldNames(producedDataType).stream()
                        .filter(this.partitionKeys::contains)
                        .collect(Collectors.toList());

        // Compute the physical projection and the physical data type, that is
        // the type without partition columns and metadata in the same order of the schema
        DataType physicalDataType = physicalRowDataType;
        final Projection partitionKeysProjections =
                Projection.fromFieldNames(physicalDataType, partitionKeysToExtract);
        final Projection physicalProjections =
                (projectFields != null
                                ? Projection.of(projectFields)
                                : Projection.all(physicalDataType))
                        .difference(partitionKeysProjections);
        physicalDataType =
                partitionKeysProjections.complement(physicalDataType).project(physicalDataType);

        if (bulkReaderFormat != null) {
            if (bulkReaderFormat instanceof BulkDecodingFormat
                    && filters != null
                    && filters.size() > 0) {
                ((BulkDecodingFormat<RowData>) bulkReaderFormat).applyFilters(filters);
            }

            BulkFormat<RowData, FileSourceSplit> format;
            if (bulkReaderFormat instanceof ProjectableDecodingFormat) {
                format =
                        ((ProjectableDecodingFormat<BulkFormat<RowData, FileSourceSplit>>)
                                        bulkReaderFormat)
                                .createRuntimeDecoder(
                                        sourceContext,
                                        physicalDataType,
                                        physicalProjections.toNestedIndexes());
            } else {
                format =
                        new ProjectingBulkFormat(
                                bulkReaderFormat.createRuntimeDecoder(
                                        sourceContext, physicalDataType),
                                physicalProjections.toTopLevelIndexes(),
                                sourceContext.createTypeInformation(
                                        physicalProjections.project(physicalDataType)));
            }

            format =
                    wrapBulkFormat(
                            sourceContext,
                            format,
                            producedDataType,
                            metadataToExtract,
                            partitionKeysToExtract);
            return format;
        } else if (deserializationFormat != null) {
            BulkFormat<RowData, FileSourceSplit> format;
            if (deserializationFormat instanceof ProjectableDecodingFormat) {
                format =
                        new DeserializationSchemaAdapter(
                                ((ProjectableDecodingFormat<DeserializationSchema<RowData>>)
                                                deserializationFormat)
                                        .createRuntimeDecoder(
                                                sourceContext,
                                                physicalDataType,
                                                physicalProjections.toNestedIndexes()));
            } else {
                format =
                        new ProjectingBulkFormat(
                                new DeserializationSchemaAdapter(
                                        deserializationFormat.createRuntimeDecoder(
                                                sourceContext, physicalDataType)),
                                physicalProjections.toTopLevelIndexes(),
                                sourceContext.createTypeInformation(
                                        physicalProjections.project(physicalDataType)));
            }

            format =
                    wrapBulkFormat(
                            sourceContext,
                            format,
                            producedDataType,
                            metadataToExtract,
                            partitionKeysToExtract);
            return format;
        } else {
            throw new TableException("Can not find format factory.");
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // When this table has no partition, just return a empty source.
        if (!partitionKeys.isEmpty() && getOrFetchPartitions().isEmpty()) {
            return InputFormatProvider.of(new CollectionInputFormat<>(new ArrayList<>(), null));
        }
        return createSourceProvider(createFormat(scanContext));
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        int[][] keys = context.getKeys();
        int[] keyIndices = new int[keys.length];
        int i = 0;
        for (int[] key : keys) {
            if (key.length > 1) {
                throw new UnsupportedOperationException(
                        "Hive lookup can not support nested key now.");
            }
            keyIndices[i] = key[0];
            i++;
        }

        PartitionFetcher.Context<Path> fetcherContext =
                new PartitionFetcher.Context<Path>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void open() {
                        // do nothing
                    }

                    @Override
                    public Optional<Path> getPartition(List<String> partValues) {
                        throw new UnsupportedOperationException(
                                "FileSystemTableSource does not support convert partition values to file path.");
                    }

                    @Override
                    public List<ComparablePartitionValue> getComparablePartitionValueList() {
                        throw new UnsupportedOperationException(
                                "FileSystemTableSource does not support obtain Comparable partition.");
                    }

                    @Override
                    public void close() {}
                };

        // avoid lambda capture
        final Path[] paths = paths();
        PartitionFetcher<Path> partitionFetcher = context1 -> new ArrayList<>(Arrays.asList(paths));

        PartitionReader<Path, RowData> partitionReader =
                new FileSystemBulkFormatPartitionReader(createFormat(context), new Configuration());

        FileSystemRowDataLookupFunction lookupFunction =
                new FileSystemRowDataLookupFunction<>(
                        partitionFetcher,
                        fetcherContext,
                        partitionReader,
                        (RowType) producedDataType.getLogicalType(),
                        keyIndices,
                        lookupMaxRetryTimes,
                        tableOptions.get(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL));

        return LookupFunctionProvider.of(lookupFunction);
    }

    /**
     * Wraps bulk format in a {@link FileInfoExtractorBulkFormat} and {@link LimitableBulkFormat},
     * if needed.
     */
    private BulkFormat<RowData, FileSourceSplit> wrapBulkFormat(
            DynamicTableSource.Context context,
            BulkFormat<RowData, FileSourceSplit> bulkFormat,
            DataType producedDataType,
            List<ReadableFileInfo> metadata,
            List<String> partitionKeys) {
        if (!metadata.isEmpty() || !partitionKeys.isEmpty()) {
            final List<String> producedFieldNames = DataType.getFieldNames(producedDataType);
            final Map<String, FileInfoAccessor> metadataColumns =
                    IntStream.range(0, metadata.size())
                            .mapToObj(
                                    i -> {
                                        // Access metadata columns from the back because the
                                        // names are decided by the planner
                                        final int columnPos =
                                                producedFieldNames.size() - metadata.size() + i;
                                        return entry(
                                                producedFieldNames.get(columnPos),
                                                metadata.get(i).getAccessor());
                                    })
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            bulkFormat =
                    new FileInfoExtractorBulkFormat(
                            bulkFormat,
                            producedDataType,
                            context.createTypeInformation(producedDataType),
                            metadataColumns,
                            partitionKeys,
                            defaultPartName);
        }
        bulkFormat = LimitableBulkFormat.create(bulkFormat, limit);
        return bulkFormat;
    }

    private SourceProvider createSourceProvider(BulkFormat<RowData, FileSourceSplit> bulkFormat) {
        final FileSource.FileSourceBuilder<RowData> fileSourceBuilder =
                FileSource.forBulkFileFormat(bulkFormat, paths());

        tableOptions
                .getOptional(FileSystemConnectorOptions.SOURCE_MONITOR_INTERVAL)
                .ifPresent(fileSourceBuilder::monitorContinuously);

        return SourceProvider.of(fileSourceBuilder.build());
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

    @Override
    public ChangelogMode getChangelogMode() {
        if (bulkReaderFormat != null) {
            return bulkReaderFormat.getChangelogMode();
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
    public TableStats reportStatistics() {
        try {
            // only support BOUNDED source
            Optional<Duration> monitorIntervalOpt =
                    tableOptions.getOptional(FileSystemConnectorOptions.SOURCE_MONITOR_INTERVAL);
            if (monitorIntervalOpt.isPresent() && monitorIntervalOpt.get().toMillis() <= 0) {
                return TableStats.UNKNOWN;
            }
            if (tableOptions.get(FileSystemConnectorOptions.SOURCE_REPORT_STATISTICS)
                    == FileSystemConnectorOptions.FileStatisticsType.NONE) {
                return TableStats.UNKNOWN;
            }

            // use NonSplittingRecursiveEnumerator to get all files
            NonSplittingRecursiveEnumerator enumerator = new NonSplittingRecursiveEnumerator();
            Collection<FileSourceSplit> splits = enumerator.enumerateSplits(paths(), 1);
            List<Path> files =
                    splits.stream().map(FileSourceSplit::path).collect(Collectors.toList());

            if (bulkReaderFormat instanceof FileBasedStatisticsReportableInputFormat) {
                TableStats tableStats =
                        ((FileBasedStatisticsReportableInputFormat) bulkReaderFormat)
                                .reportStatistics(files, producedDataType);
                if (tableStats.equals(TableStats.UNKNOWN)) {
                    return tableStats;
                }
                return limit == null
                        ? tableStats
                        : new TableStats(Math.min(limit, tableStats.getRowCount()));
            } else if (deserializationFormat instanceof FileBasedStatisticsReportableInputFormat) {
                TableStats tableStats =
                        ((FileBasedStatisticsReportableInputFormat) deserializationFormat)
                                .reportStatistics(files, producedDataType);
                if (tableStats.equals(TableStats.UNKNOWN)) {
                    return tableStats;
                }
                return limit == null
                        ? tableStats
                        : new TableStats(Math.min(limit, tableStats.getRowCount()));
            } else {
                return TableStats.UNKNOWN;
            }
        } catch (Exception e) {
            LOG.warn(
                    "Reporting statistics failed for file system table source: {}", e.getMessage());
            return TableStats.UNKNOWN;
        }
    }

    @Override
    public FileSystemTableSource copy() {
        return new FileSystemTableSource(
                tableIdentifier,
                physicalRowDataType,
                partitionKeys,
                tableOptions,
                bulkReaderFormat,
                deserializationFormat,
                lookupMaxRetryTimes);
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
        this.producedDataType = producedDataType;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
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
                "file.path",
                DataTypes.STRING().notNull(),
                new FileInfoAccessor() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object getValue(FileSourceSplit split) {
                        return StringData.fromString(split.path().getPath());
                    }
                }),
        FILENAME(
                "file.name",
                DataTypes.STRING().notNull(),
                new FileInfoAccessor() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object getValue(FileSourceSplit split) {
                        return StringData.fromString(
                                Paths.get(split.path().getPath()).getFileName().toString());
                    }
                }),
        SIZE(
                "file.size",
                DataTypes.BIGINT().notNull(),
                new FileInfoAccessor() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object getValue(FileSourceSplit split) {
                        return split.fileSize();
                    }
                }),
        MODIFICATION_TIME(
                "file.modification-time",
                DataTypes.TIMESTAMP_LTZ(3).notNull(),
                new FileInfoAccessor() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object getValue(FileSourceSplit split) {
                        return TimestampData.fromEpochMillis(split.fileModificationTime());
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
            return Arrays.stream(ReadableFileInfo.values())
                    .filter(readableFileInfo -> readableFileInfo.getKey().equals(key))
                    .findFirst()
                    .orElseThrow(
                            () ->
                                    new IllegalArgumentException(
                                            "Cannot resolve the provided ReadableMetadata key"));
        }
    }
}
