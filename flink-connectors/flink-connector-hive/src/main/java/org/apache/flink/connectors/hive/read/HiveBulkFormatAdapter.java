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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.ArrayResultIterator;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.orc.OrcColumnarRowFileInputFormat;
import org.apache.flink.orc.nohive.OrcNoHiveColumnarRowInputFormat;
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.PartitionFieldExtractor;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.file.src.util.CheckpointedPosition.NO_OFFSET;
import static org.apache.flink.table.data.vector.VectorizedColumnBatch.DEFAULT_SIZE;

/**
 * A BulkFormat implementation for HiveSource. This implementation delegates reading to other
 * BulkFormat instances, because different hive partitions may need different BulkFormat to do the
 * reading.
 */
public class HiveBulkFormatAdapter implements BulkFormat<RowData, HiveSourceSplit> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(HiveBulkFormatAdapter.class);

    // schema evolution configs are not available in older versions of IOConstants, let's define
    // them ourselves
    private static final String SCHEMA_EVOLUTION_COLUMNS = "schema.evolution.columns";
    private static final String SCHEMA_EVOLUTION_COLUMNS_TYPES = "schema.evolution.columns.types";

    private static final PartitionFieldExtractor<HiveSourceSplit> PARTITION_FIELD_EXTRACTOR =
            (split, fieldName, fieldType) ->
                    split.getHiveTablePartition().getPartitionSpec().get(fieldName);

    private final JobConfWrapper jobConfWrapper;
    private final List<String> partitionKeys;
    private final String[] fieldNames;
    private final DataType[] fieldTypes;
    private final String hiveVersion;
    private final HiveShim hiveShim;
    private final RowType producedRowType;
    private final boolean useMapRedReader;

    public HiveBulkFormatAdapter(
            JobConfWrapper jobConfWrapper,
            List<String> partitionKeys,
            String[] fieldNames,
            DataType[] fieldTypes,
            String hiveVersion,
            RowType producedRowType,
            boolean useMapRedReader) {
        this.jobConfWrapper = jobConfWrapper;
        this.partitionKeys = partitionKeys;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.hiveVersion = hiveVersion;
        this.hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
        this.producedRowType = producedRowType;
        this.useMapRedReader = useMapRedReader;
    }

    @Override
    public Reader<RowData> createReader(Configuration config, HiveSourceSplit split)
            throws IOException {
        return createBulkFormatForSplit(split).createReader(config, split);
    }

    @Override
    public Reader<RowData> restoreReader(Configuration config, HiveSourceSplit split)
            throws IOException {
        return createBulkFormatForSplit(split).restoreReader(config, split);
    }

    @Override
    public boolean isSplittable() {
        return true;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(producedRowType);
    }

    private RowType tableRowType() {
        LogicalType[] types =
                Arrays.stream(fieldTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new);
        return RowType.of(types, fieldNames);
    }

    private BulkFormat<RowData, ? super HiveSourceSplit> createBulkFormatForSplit(
            HiveSourceSplit split) {
        if (!useMapRedReader && useParquetVectorizedRead(split.getHiveTablePartition())) {
            return ParquetColumnarRowInputFormat.createPartitionedFormat(
                    jobConfWrapper.conf(),
                    producedRowType,
                    partitionKeys,
                    PARTITION_FIELD_EXTRACTOR,
                    DEFAULT_SIZE,
                    hiveVersion.startsWith("3"),
                    false);
        } else if (!useMapRedReader && useOrcVectorizedRead(split.getHiveTablePartition())) {
            return createOrcFormat();
        } else {
            return new HiveMapRedBulkFormat();
        }
    }

    private OrcColumnarRowFileInputFormat<?, HiveSourceSplit> createOrcFormat() {
        return hiveVersion.startsWith("1.")
                ? OrcNoHiveColumnarRowInputFormat.createPartitionedFormat(
                        jobConfWrapper.conf(),
                        tableRowType(),
                        partitionKeys,
                        PARTITION_FIELD_EXTRACTOR,
                        computeSelectedFields(),
                        Collections.emptyList(),
                        DEFAULT_SIZE)
                : OrcColumnarRowFileInputFormat.createPartitionedFormat(
                        OrcShim.createShim(hiveVersion),
                        jobConfWrapper.conf(),
                        tableRowType(),
                        partitionKeys,
                        PARTITION_FIELD_EXTRACTOR,
                        computeSelectedFields(),
                        Collections.emptyList(),
                        DEFAULT_SIZE);
    }

    private boolean useOrcVectorizedRead(HiveTablePartition partition) {
        boolean isOrc =
                partition
                        .getStorageDescriptor()
                        .getSerdeInfo()
                        .getSerializationLib()
                        .toLowerCase()
                        .contains("orc");
        if (!isOrc) {
            return false;
        }

        for (RowType.RowField field : producedRowType.getFields()) {
            if (isVectorizationUnsupported(field.getType())) {
                LOG.info(
                        "Fallback to hadoop mapred reader, unsupported field type: "
                                + field.getType());
                return false;
            }
        }

        LOG.info("Use flink orc ColumnarRowData reader.");
        return true;
    }

    private boolean useParquetVectorizedRead(HiveTablePartition partition) {
        boolean isParquet =
                partition
                        .getStorageDescriptor()
                        .getSerdeInfo()
                        .getSerializationLib()
                        .toLowerCase()
                        .contains("parquet");
        if (!isParquet) {
            return false;
        }

        for (RowType.RowField field : producedRowType.getFields()) {
            if (isVectorizationUnsupported(field.getType())) {
                LOG.info(
                        "Fallback to hadoop mapred reader, unsupported field type: "
                                + field.getType());
                return false;
            }
        }

        LOG.info("Use flink parquet ColumnarRowData reader.");
        return true;
    }

    private static boolean isVectorizationUnsupported(LogicalType t) {
        switch (t.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return false;
            case TIMESTAMP_WITH_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
            case NULL:
            case RAW:
            case SYMBOL:
            default:
                return true;
        }
    }

    private class HiveMapRedBulkFormat implements BulkFormat<RowData, HiveSourceSplit> {

        private static final long serialVersionUID = 1L;

        @Override
        public Reader<RowData> createReader(Configuration config, HiveSourceSplit split)
                throws IOException {
            return new HiveReader(split);
        }

        @Override
        public Reader<RowData> restoreReader(Configuration config, HiveSourceSplit split)
                throws IOException {
            assert split.getReaderPosition().isPresent();
            HiveReader hiveReader = new HiveReader(split);
            hiveReader.seek(split.getReaderPosition().get().getRecordsAfterOffset());
            return hiveReader;
        }

        @Override
        public boolean isSplittable() {
            return true;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return InternalTypeInfo.of(producedRowType);
        }
    }

    private class HiveReader implements BulkFormat.Reader<RowData> {

        private final HiveMapredSplitReader hiveMapredSplitReader;
        private final RowDataSerializer serializer;
        private final int[] selectedFields;
        private long numRead = 0;

        private HiveReader(HiveSourceSplit split) throws IOException {
            selectedFields = computeSelectedFields();
            JobConf clonedConf = new JobConf(jobConfWrapper.conf());
            addSchemaToConf(clonedConf);
            HiveTableInputSplit oldSplit =
                    new HiveTableInputSplit(
                            -1, split.toMapRedSplit(), clonedConf, split.getHiveTablePartition());
            hiveMapredSplitReader =
                    new HiveMapredSplitReader(
                            clonedConf,
                            partitionKeys,
                            fieldTypes,
                            selectedFields,
                            oldSplit,
                            hiveShim);
            serializer = new RowDataSerializer(producedRowType);
        }

        @Override
        public RecordIterator<RowData> readBatch() throws IOException {
            RowData[] records = new RowData[DEFAULT_SIZE];
            final long skipCount = numRead;
            int num = 0;
            while (!hiveMapredSplitReader.reachedEnd() && num < DEFAULT_SIZE) {
                records[num++] = serializer.copy(nextRecord());
            }
            if (num == 0) {
                return null;
            }

            ArrayResultIterator<RowData> iterator = new ArrayResultIterator<>();
            iterator.set(records, num, NO_OFFSET, skipCount);
            return iterator;
        }

        @Override
        public void close() throws IOException {
            hiveMapredSplitReader.close();
        }

        private RowData nextRecord() throws IOException {
            RowData res = hiveMapredSplitReader.nextRecord(null);
            numRead++;
            return res;
        }

        private void seek(long toSkip) throws IOException {
            while (!hiveMapredSplitReader.reachedEnd() && toSkip > 0) {
                nextRecord();
                toSkip--;
            }
        }

        // Hive readers may rely on the schema info in configuration
        private void addSchemaToConf(JobConf jobConf) {
            // set columns/types -- including partition cols
            List<String> typeStrs =
                    Arrays.stream(fieldTypes)
                            .map(t -> HiveTypeUtil.toHiveTypeInfo(t, true).toString())
                            .collect(Collectors.toList());
            jobConf.set(IOConstants.COLUMNS, String.join(",", fieldNames));
            jobConf.set(IOConstants.COLUMNS_TYPES, String.join(",", typeStrs));
            // set schema evolution -- excluding partition cols
            int numNonPartCol = fieldNames.length - partitionKeys.size();
            jobConf.set(
                    SCHEMA_EVOLUTION_COLUMNS,
                    String.join(",", Arrays.copyOfRange(fieldNames, 0, numNonPartCol)));
            jobConf.set(
                    SCHEMA_EVOLUTION_COLUMNS_TYPES,
                    String.join(",", typeStrs.subList(0, numNonPartCol)));

            // in older versions, parquet reader also expects the selected col indices in conf,
            // excluding part cols
            String readColIDs =
                    Arrays.stream(selectedFields)
                            .filter(i -> i < numNonPartCol)
                            .mapToObj(String::valueOf)
                            .collect(Collectors.joining(","));
            jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIDs);
        }
    }

    // compute indices of selected fields according to the produced type
    private int[] computeSelectedFields() {
        int[] selectedFields = new int[producedRowType.getFieldCount()];
        for (int i = 0; i < selectedFields.length; i++) {
            String name = producedRowType.getFieldNames().get(i);
            int index = Arrays.asList(fieldNames).indexOf(name);
            Preconditions.checkState(
                    index >= 0,
                    "Produced field name %s not found in table schema fields %s",
                    name,
                    Arrays.toString(fieldNames));
            selectedFields[i] = index;
        }
        return selectedFields;
    }
}
